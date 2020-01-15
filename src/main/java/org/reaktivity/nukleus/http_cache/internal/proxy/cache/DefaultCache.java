/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE_0;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.NO_STORE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isMatchByEtag;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.NOT_MODIFIED_304;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.OK_200;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferenceApplied;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.LINK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.METHOD;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.TRANSFER_ENCODING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.net.URI;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyFactory;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpProxyCacheableRequestGroup;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.route.RouteManager;

public class DefaultCache
{
    private static final Pattern LINK_URL_PATTERN =
        Pattern.compile(
            "((<(?<scheme>https?):/)?/?(?<hostname>[^:/\\s]+)(?<port>:(\\d+))?(?<path>[\\w\\-.]*[^#?\\s]+).*>;" +
            ".*(rel=\"(collection|items)\"))");

    final ArrayFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ArrayFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControl = new CacheControl();
    final CacheControl cachedRequestCacheControl = new CacheControl();

    private final BufferPool cachedRequestBufferPool;
    private final BufferPool cachedResponseBufferPool;
    private final BufferPool cacheBufferPool;

    private final Writer writer;
    private final Int2ObjectHashMap<DefaultCacheEntry> cachedEntriesByRequestHash;
    private final Int2ObjectHashMap<Int2ObjectHashMap<DefaultCacheEntry>> cachedEntriesByRequestHashWithoutQuery;

    private final LongConsumer entryCount;
    private final LongSupplier supplyTraceId;
    private final HttpCacheCounters counters;
    private final int allowedSlots;

    public DefaultCache(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool cacheBufferPool,
        HttpCacheCounters counters,
        LongConsumer entryCount,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        int allowedCachePercentage,
        int cacheCapacity)
    {
        assert allowedCachePercentage >= 0 && allowedCachePercentage <= 100;
        this.cacheBufferPool = cacheBufferPool;
        this.entryCount = entryCount;
        this.writer = new Writer(router, supplyTypeId, writeBuffer);
        this.cachedRequestBufferPool = new CountingBufferPool(
                cacheBufferPool,
                counters.supplyCounter.apply("http-cache.cached.request.acquires"),
                counters.supplyCounter.apply("http-cache.cached.request.releases"));
        this.cachedResponseBufferPool = new CountingBufferPool(
                cacheBufferPool.duplicate(),
                counters.supplyCounter.apply("http-cache.cached.response.acquires"),
                counters.supplyCounter.apply("http-cache.cached.response.releases"));
        this.cachedEntriesByRequestHash = new Int2ObjectHashMap<>();
        this.cachedEntriesByRequestHashWithoutQuery = new Int2ObjectHashMap<>();
        this.counters = counters;
        this.supplyTraceId = requireNonNull(supplyTraceId);
        int totalSlots = cacheCapacity / cacheBufferPool.slotCapacity();
        this.allowedSlots = (totalSlots * allowedCachePercentage) / 100;
    }

    public BufferPool getResponsePool()
    {
        return cachedResponseBufferPool;
    }

    public DefaultCacheEntry get(
        int requestHash)
    {
        return cachedEntriesByRequestHash.get(requestHash);
    }

    public DefaultCacheEntry supply(
        int requestHash,
        short authScope,
        String requestURL)
    {
        final int requestHashWithoutQuery = generateRequestHashWithoutQuery(requestURL);
        Int2ObjectHashMap<DefaultCacheEntry> cachedEntriesByRequestHashFromWithoutQueryList =
            cachedEntriesByRequestHashWithoutQuery.computeIfAbsent(requestHashWithoutQuery, l -> new Int2ObjectHashMap<>());

        DefaultCacheEntry cacheEntry = computeCacheEntryIfAbsent(requestHash, authScope, requestHashWithoutQuery);
        cachedEntriesByRequestHashFromWithoutQueryList.put(requestHash, cacheEntry);
        cachedEntriesByRequestHash.put(requestHash, cacheEntry);
        return cacheEntry;
    }

    private int generateRequestHashWithoutQuery(
        String requestURL)
    {
        final URI requestURI = URI.create(requestURL);
        return String.format("%s://%s%s",
                             requestURI.getScheme(),
                             requestURI.getAuthority(),
                             requestURI.getPath()).hashCode();
    }


    public boolean matchCacheableRequest(
        ArrayFW<HttpHeaderFW> requestHeaders,
        short authScope,
        int requestHash)
    {
        final DefaultCacheEntry cacheEntry = cachedEntriesByRequestHash.get(requestHash);

        return satisfiedByCache(requestHeaders) &&
               cacheEntry != null &&
               (cacheEntry.etag() != null || cacheEntry.isResponseCompleted()) &&
               cacheEntry.canServeRequest(requestHeaders, authScope);
    }

    public void purge(
        int requestHash)
    {
        DefaultCacheEntry cacheEntry = cachedEntriesByRequestHash.remove(requestHash);
        if (cacheEntry != null)
        {
            final int requestHashWithoutQuery = cacheEntry.requestHashWithoutQuery();
            cachedEntriesByRequestHashWithoutQuery.computeIfPresent(
                requestHashWithoutQuery, (h, m) -> m.remove(requestHash) != null && m.isEmpty() ? null : m);

            entryCount.accept(-1);
            cacheEntry.purge();
            counters.responsesPurged.getAsLong();
        }
    }

    public void invalidateCacheEntryIfNecessary(
        HttpCacheProxyFactory factory,
        int requestHash,
        String requestURL,
        long traceId,
        ArrayFW<HttpHeaderFW> headers)
    {
        DefaultCacheEntry cacheEntry = cachedEntriesByRequestHash.remove(requestHash);
        if (cacheEntry != null)
        {
            cacheEntry.invalidate();
        }

        headers.forEach(header -> invalidateLinkCacheEntry(factory, requestURL, traceId, header));
    }

    private void invalidateLinkCacheEntry(
        HttpCacheProxyFactory factory,
        String requestURL,
        long traceId,
        HttpHeaderFW header)
    {
        final String linkName = header.name().asString();
        final String linkValue = header.value().asString();
        if (LINK.equals(linkName))
        {
            for (String linkTarget: linkValue.split("\\s*,\\s*"))
            {
                Matcher matcher = LINK_URL_PATTERN.matcher(linkTarget);
                if (matcher.matches())
                {
                    final URI requestURI = URI.create(requestURL);
                    final String linkTargetScheme = matcher.group("scheme");
                    final String linkTargetHostname = matcher.group("hostname");

                    if ((linkTargetScheme != null && linkTargetHostname != null) &&
                        (!linkTargetScheme.equals(requestURI.getScheme()) || !linkTargetHostname.equals(requestURI.getHost())))
                    {
                        return;
                    }

                    final String linkTargetFullUrl = String.format("%s://%s%s", requestURI.getScheme(),
                        requestURI.getAuthority(),  matcher.group("path"));
                    final int requestHashWithoutQuery = generateRequestHashWithoutQuery(linkTargetFullUrl);
                    Int2ObjectHashMap<DefaultCacheEntry> requestHashWithoutQueryList =
                        cachedEntriesByRequestHashWithoutQuery.get(requestHashWithoutQuery);
                    if (requestHashWithoutQueryList != null)
                    {
                        requestHashWithoutQueryList.forEach((hash, entry) ->
                        {
                            final HttpProxyCacheableRequestGroup requestGroup = factory.getRequestGroup(hash);
                            if (requestGroup != null)
                            {
                                requestGroup.onCacheEntryInvalidated(traceId);
                            }
                            entry.invalidate();
                        });
                    }
                }
            }
        }
    }

    public boolean checkTrailerToRetry(
        String ifNoneMatch,
        DefaultCacheEntry cacheEntry)
    {
        ArrayFW<HttpHeaderFW> requestHeaders = cacheEntry.getRequestHeaders();
        ArrayFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();
        if (isPreferWait(requestHeaders) &&
            !isPreferenceApplied(responseHeaders) &&
            requestHeaders.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString()) && h.value().asString().contains(MAX_AGE_0)))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String newEtag = cacheEntry.etag();
            assert status != null;

            if (ifNoneMatch != null && newEtag != null)
            {
                return status.equals(HttpStatus.OK_200) && isMatchByEtag(requestHeaders, newEtag);
            }
        }

        return false;
    }

    public boolean checkToRetry(
        ArrayFW<HttpHeaderFW> requestHeaders,
        ArrayFW<HttpHeaderFW> responseHeaders,
        String ifNoneMatch,
        int requestHash)
    {
        if (isPreferWait(requestHeaders) &&
            !isPreferenceApplied(responseHeaders) &&
            requestHeaders.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString()) &&
                                         h.value().asString().contains(MAX_AGE_0)))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String newEtag = getHeader(responseHeaders, ETAG);
            boolean etagMatches = false;
            assert status != null;

            if (ifNoneMatch != null && newEtag != null)
            {
                etagMatches = status.equals(HttpStatus.OK_200) && isMatchByEtag(requestHeaders, newEtag);

                if (etagMatches)
                {
                    DefaultCacheEntry cacheEntry = cachedEntriesByRequestHash.get(requestHash);
                    if (cacheEntry != null)
                    {
                        cacheEntry.updateResponseHeader(status, responseHeaders);
                    }
                }
            }

            return status.equals(NOT_MODIFIED_304) || etagMatches;
        }

        return false;
    }

    public void send304(
        int requestHash,
        String etag,
        String preferWait,
        MessageConsumer reply,
        long routeId,
        long replyId,
        long authorization,
        boolean promiseNextPollRequest,
        long traceId)
    {

        if (preferWait != null)
        {
            writer.doHttpResponse(
                reply,
                routeId,
                replyId,
                traceId,
                e -> e.item(h -> h.name(STATUS).value(NOT_MODIFIED_304))
                      .item(h -> h.name(ETAG).value(etag))
                      .item(h -> h.name(PREFERENCE_APPLIED).value(preferWait))
                      .item(h -> h.name(ACCESS_CONTROL_EXPOSE_HEADERS).value(String.format("%s, %s", PREFERENCE_APPLIED, ETAG))));

            if (promiseNextPollRequest)
            {
                DefaultCacheEntry cacheEntry = get(requestHash);
                writer.doHttpPushPromise(
                    reply,
                    routeId,
                    replyId,
                    authorization,
                    cacheEntry.getRequestHeaders(),
                    cacheEntry.getCachedResponseHeaders(),
                    cacheEntry.etag());
            }
        }
        else
        {
            writer.doHttpResponse(
                reply,
                routeId,
                replyId,
                traceId,
                e -> e.item(h -> h.name(STATUS).value(NOT_MODIFIED_304))
                      .item(h -> h.name(ETAG).value(etag)));
        }

        writer.doHttpEnd(reply, routeId, replyId, traceId);
    }

    public boolean isRequestCacheable(
        ArrayFW<HttpHeaderFW> headers)
    {
        return cacheBufferPool.acquiredSlots() <= allowedSlots &&
            !headers.anyMatch(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                switch (name)
                {
                case CACHE_CONTROL:
                    return value.contains(CacheDirectives.NO_STORE);
                case METHOD:
                    return !HttpMethods.GET.equalsIgnoreCase(value);
                case TRANSFER_ENCODING:
                    return true;
                default:
                    return false;
                }
            });
    }

    public void purgeEntriesForNonPendingRequests()
    {
        cachedEntriesByRequestHash.forEach((requestHash, cacheEntry) ->
        {
            if (cacheEntry.getSubscribers() == 0)
            {
                purge(requestHash);
            }
        });
    }

    public boolean updateResponseHeaderIfNecessary(
        int requestHash,
        ArrayFW<HttpHeaderFW> responseHeaders)
    {
        String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
        boolean isSelectedForUpdate = NOT_MODIFIED_304.equals(status) || OK_200.equals(status);

        if (isSelectedForUpdate)
        {
            DefaultCacheEntry cacheEntry =  get(requestHash);
            if (cacheEntry != null)
            {
                cacheEntry.updateResponseHeader(status, responseHeaders);
            }
        }

        return isSelectedForUpdate;
    }

    public boolean satisfiedByCache(
        ArrayFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            return CACHE_CONTROL.equals(name) &&
                   (value.contains(CacheDirectives.NO_CACHE) ||
                    value.contains(MAX_AGE_0) ||
                    value.contains(NO_STORE));
        });
    }


    private DefaultCacheEntry computeCacheEntryIfAbsent(
        int requestHash,
        short authScope,
        int collectionHash)
    {
        DefaultCacheEntry entry = get(requestHash);
        if (entry == null)
        {
            entryCount.accept(1);
            return new DefaultCacheEntry(this,
                                         requestHash,
                                         authScope,
                                         collectionHash,
                                         cachedRequestBufferPool,
                                         cachedResponseBufferPool);
        }

        return entry;
    }
}
