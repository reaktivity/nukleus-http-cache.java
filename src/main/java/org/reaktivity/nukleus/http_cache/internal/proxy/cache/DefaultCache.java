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

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.METHOD;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.TRANSFER_ENCODING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
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
    final ArrayFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ArrayFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();

    private final BufferPool cachedRequestBufferPool;
    private final BufferPool cachedResponseBufferPool;
    private final BufferPool cacheBufferPool;

    private final Writer writer;
    private final Int2ObjectHashMap<DefaultCacheEntry> cachedEntries;

    private final LongConsumer entryCount;
    private final LongSupplier supplyTraceId;
    public final HttpCacheCounters counters;
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
        this.cachedEntries = new Int2ObjectHashMap<>();
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
        return cachedEntries.get(requestHash);
    }

    public DefaultCacheEntry supply(
        int requestHash,
        short authScope)
    {
        DefaultCacheEntry entry = get(requestHash);
        if (entry == null)
        {
            entry = newCacheEntry(requestHash, authScope);
            cachedEntries.put(requestHash, entry);
        }

        return entry;
    }

    public boolean matchCacheableResponse(
        int requestHash,
        String newEtag,
        boolean hasIfNoneMatch)
    {
        DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
        return hasIfNoneMatch &&
               cacheEntry != null &&
               cacheEntry.etag() != null &&
               cacheEntry.etag().equals(newEtag);
    }

    public boolean matchCacheableRequest(
        ArrayFW<HttpHeaderFW> requestHeaders,
        short authScope,
        int requestHash)
    {
        final DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
        return satisfiedByCache(requestHeaders) &&
                cacheEntry != null &&
                cacheEntry.canServeRequest(requestHeaders, authScope);

    }

    public void purge(
        int requestHash)
    {
        DefaultCacheEntry cacheEntry = this.cachedEntries.remove(requestHash);
        entryCount.accept(-1);
        if (cacheEntry != null)
        {
            cacheEntry.purge();
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
                    DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
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
        String etag,
        String preferWait,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                              currentTimeMillis(), acceptReplyId, "304");
        }

        if (preferWait != null)
        {
            writer.doHttpResponse(acceptReply,
                acceptRouteId,
                acceptReplyId,
                supplyTraceId.getAsLong(),
                e -> e.item(h -> h.name(STATUS).value(NOT_MODIFIED_304))
                      .item(h -> h.name(ETAG).value(etag))
                      .item(h -> h.name(PREFERENCE_APPLIED).value(preferWait))
                      .item(h -> h.name(ACCESS_CONTROL_EXPOSE_HEADERS).value(PREFERENCE_APPLIED))
                      .item(h -> h.name(ACCESS_CONTROL_EXPOSE_HEADERS).value(ETAG)));
        }
        else
        {
            writer.doHttpResponse(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                supplyTraceId.getAsLong(),
                e -> e.item(h -> h.name(STATUS).value(NOT_MODIFIED_304))
                      .item(h -> h.name(ETAG).value(etag)));
        }
        // count all responses
        counters.responses.getAsLong();
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
        cachedEntries.forEach((requestHash, cacheEntry) ->
        {
            if (cacheEntry.getSubscribers() == 0)
            {
                this.purge(requestHash);
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


    private boolean satisfiedByCache(
        ArrayFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
            case CACHE_CONTROL:
                // TODO remove need for max-age=0 (Currently can't handle multiple outstanding cache updates)
                return value.contains(CacheDirectives.NO_CACHE) ||
                       value.contains(MAX_AGE_0) ||
                       value.contains(NO_STORE);
            default:
                return false;
            }
        });
    }

    private DefaultCacheEntry newCacheEntry(
        int requestHash,
        short authScope)
    {
        entryCount.accept(1);
        return new DefaultCacheEntry(
            this,
            requestHash,
            authScope,
            cachedRequestBufferPool,
            cachedResponseBufferPool);
    }
}
