/**
 * Copyright 2016-2020 The Reaktivity Project
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
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyFactory;
import org.reaktivity.nukleus.http_cache.internal.stream.HttpProxyCacheableRequestGroup;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.route.RouteManager;

public class DefaultCache
{
    private static final Pattern LINK_URL_PATTERN =
        Pattern.compile(
            "((<(?<scheme>https?):/)?/?(?<hostname>[^:/\\s]+)(?<port>:(\\d+))?(?<path>[\\w\\-.]*[^#?\\s]+).*>;" +
            ".*(rel=\"(collection|items)\"))");

    final Array32FW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final Array32FW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControl = new CacheControl();
    final CacheControl cachedRequestCacheControl = new CacheControl();

    private final BufferPool cachedRequestBufferPool;
    private final BufferPool cachedResponseBufferPool;
    private final BufferPool cacheBufferPool;

    private final Writer writer;
    private final Int2ObjectHashMap<DefaultCacheEntry> cachedEntriesByRequestHash;
    private final Int2ObjectHashMap<FrequencyBucket> frequencies;
    private final Int2ObjectHashMap<Int2ObjectHashMap<DefaultCacheEntry>> cachedEntriesByRequestHashWithoutQuery;

    private final HttpCacheCounters counters;
    private final int allowedSlots;
    private final int allowedCacheEvictionCount;

    public DefaultCache(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool cacheBufferPool,
        HttpCacheCounters counters,
        ToIntFunction<String> supplyTypeId,
        int allowedCachePercentage,
        int cacheCapacity,
        int allowedCacheEvictionCount)
    {
        assert allowedCachePercentage >= 0 && allowedCachePercentage <= 100;
        this.cacheBufferPool = cacheBufferPool;
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
        this.frequencies = new Int2ObjectHashMap<>();
        this.cachedEntriesByRequestHashWithoutQuery = new Int2ObjectHashMap<>();
        this.counters = counters;
        int totalSlots = cacheCapacity / cacheBufferPool.slotCapacity();
        this.allowedSlots = (totalSlots * allowedCachePercentage) / 100;
        this.allowedCacheEvictionCount = allowedCacheEvictionCount;
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

    public DefaultCacheEntry lookup(
        int requestHash)
    {
        DefaultCacheEntry entry = cachedEntriesByRequestHash.get(requestHash);
        if (entry != null)
        {
            incrementFrequency(entry);
        }
        return entry;
    }

    public DefaultCacheEntry supply(
        int requestHash,
        short authScope,
        String requestURL)
    {
        final int requestHashWithoutQuery = generateRequestHashWithoutQuery(requestURL);
        Int2ObjectHashMap<DefaultCacheEntry> cachedEntriesByRequestHashFromWithoutQueryList =
            cachedEntriesByRequestHashWithoutQuery.computeIfAbsent(requestHashWithoutQuery, l -> new Int2ObjectHashMap<>());

        DefaultCacheEntry entry = cachedEntriesByRequestHash.get(requestHash);
        if (entry == null)
        {
            entry = new DefaultCacheEntry(this, requestHash, authScope, requestHashWithoutQuery, cachedRequestBufferPool,
                                          cachedResponseBufferPool);
            cachedEntriesByRequestHash.put(requestHash, entry);
            counters.cacheEntries.accept(1);
        }

        cachedEntriesByRequestHashFromWithoutQueryList.put(requestHash, entry);

        return entry;
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
        Array32FW<HttpHeaderFW> requestHeaders,
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
        DefaultCacheEntry entry = cachedEntriesByRequestHash.remove(requestHash);
        assert entry != null;

        if (entry.frequencyParent() != null)
        {
            entry.frequencyParent().entries().remove(entry);
        }

        final int requestHashWithoutQuery = entry.requestHashWithoutQuery();
        cachedEntriesByRequestHashWithoutQuery.computeIfPresent(
            requestHashWithoutQuery, (h, m) -> m.remove(requestHash) != null && m.isEmpty() ? null : m);

        entry.purge();
        counters.cacheEntries.accept(-1);
        counters.responsesPurged.getAsLong();
    }

    public void invalidateCacheEntryIfNecessary(
        HttpCacheProxyFactory factory,
        int requestHash,
        String requestURL,
        long traceId,
        Array32FW<HttpHeaderFW> headers)
    {
        DefaultCacheEntry cacheEntry = cachedEntriesByRequestHash.get(requestHash);
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
        Array32FW<HttpHeaderFW> requestHeaders = cacheEntry.getRequestHeaders();
        Array32FW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();
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
        Array32FW<HttpHeaderFW> requestHeaders,
        Array32FW<HttpHeaderFW> responseHeaders,
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
        long traceId,
        long authorization,
        boolean promiseNextPollRequest)
    {
        DefaultCacheEntry cacheEntry = lookup(requestHash);
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

    public boolean isCacheFull()
    {
        return cacheBufferPool.acquiredSlots() >= allowedSlots;
    }

    public boolean isRequestCacheable(
        Array32FW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
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

    public void purgeEntriesForNonPendingRequests(
        Set<Integer> requestHashes)
    {
        for (int count = 1; count <= allowedCacheEvictionCount;)
        {
            final FrequencyBucket frequencyBucket = frequencies.get(count);

            if (frequencyBucket != null)
            {
                for (DefaultCacheEntry entry : frequencyBucket.entries())
                {
                    final int requestHash = entry.requestHash();
                    if (!requestHashes.contains(requestHash))
                    {
                        if (count <= allowedCacheEvictionCount)
                        {
                            purge(requestHash);
                            count++;
                        }
                        else
                        {
                            return;
                        }
                    }
                }
            }
        }
        counters.cachePurgeAttempts.getAsLong();
    }

    public void updateResponseHeaderIfNecessary(
        int requestHash,
        Array32FW<HttpHeaderFW> responseHeaders)
    {
        String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
        boolean isSelectedForUpdate = NOT_MODIFIED_304.equals(status) || OK_200.equals(status);

        if (isSelectedForUpdate)
        {
            DefaultCacheEntry entry =  cachedEntriesByRequestHash.get(requestHash);
            if (entry != null)
            {
                entry.updateResponseHeader(status, responseHeaders);
            }
        }
    }

    public boolean satisfiedByCache(
        Array32FW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            return CACHE_CONTROL.equals(name) &&
                   value != null &&
                   (value.contains(CacheDirectives.NO_CACHE) ||
                    value.contains(MAX_AGE_0) ||
                    value.contains(NO_STORE));
        });
    }

    private void incrementFrequency(
        DefaultCacheEntry entry)
    {
        final FrequencyBucket frequencyBucket = entry.frequencyParent();
        final int currentParentKey = (frequencyBucket != null) ? frequencyBucket.frequency() : 0;
        final FrequencyBucket currentFrequency = frequencies.get(currentParentKey);
        int nextFrequencyAmount;
        FrequencyBucket nextFrequency = null;

        nextFrequencyAmount =  (currentFrequency == null) ? 1 : currentParentKey + 1;
        nextFrequency = frequencies.get(nextFrequencyAmount);

        if (nextFrequency == null)
        {
            nextFrequency = new FrequencyBucket(nextFrequencyAmount);
            frequencies.put(nextFrequencyAmount, nextFrequency);
            counters.frequencyBuckets.accept(1);
        }

        nextFrequency.entries().add(entry);
        entry.frequencyParent(nextFrequency);

        if (currentFrequency != null)
        {
            final ObjectHashSet<DefaultCacheEntry> entries = currentFrequency.entries();
            entries.remove(entry);

            if (entries.isEmpty())
            {
                frequencies.remove(currentParentKey);
                counters.frequencyBuckets.accept(-1);
            }
        }
    }
}
