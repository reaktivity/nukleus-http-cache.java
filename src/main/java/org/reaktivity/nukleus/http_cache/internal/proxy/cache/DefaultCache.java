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

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.satisfiedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.NOT_MODIFIED_304;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferenceApplied;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

public class DefaultCache
{
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();

    final BufferPool cachedRequestBufferPool;
    final BufferPool cachedResponseBufferPool;

    final Writer writer;
    final Int2CacheHashMapWithLRUEviction cachedEntries;

    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations;
    final LongSupplier supplyTrace;
    final SignalingExecutor executor;
    public final HttpCacheCounters counters;

    public DefaultCache(
        LongObjectBiConsumer<Runnable> scheduler,
        MutableDirectBuffer writeBuffer,
        BufferPool cacheBufferPool,
        Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations,
        HttpCacheCounters counters,
        LongConsumer entryCount,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor)
    {
        this.scheduler = scheduler;
        this.correlations = correlations;
        this.writer = new Writer(supplyTypeId, writeBuffer);
        this.cachedRequestBufferPool = new CountingBufferPool(
                cacheBufferPool,
                counters.supplyCounter.apply("http-cache.cached.request.acquires"),
                counters.supplyCounter.apply("http-cache.cached.request.releases"));
        this.cachedResponseBufferPool = new CountingBufferPool(
                cacheBufferPool.duplicate(),
                counters.supplyCounter.apply("http-cache.cached.response.acquires"),
                counters.supplyCounter.apply("http-cache.cached.response.releases"));
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.counters = counters;
        this.supplyTrace = requireNonNull(supplyTrace);
        this.executor = executor;
    }

    public DefaultCacheEntry get(
        int requestHash)
    {
        return cachedEntries.get(requestHash);
    }

    public DefaultCacheEntry supply(
        int requestHash)
    {
        DefaultCacheEntry defaultCacheEntry = cachedEntries.get(requestHash);
        if (defaultCacheEntry == null)
        {
            defaultCacheEntry = new DefaultCacheEntry(
                    this,
                    requestHash,
                    cachedRequestBufferPool,
                    cachedResponseBufferPool);
            updateCache(requestHash, defaultCacheEntry);
        }

        return defaultCacheEntry;
    }

    public boolean matchCacheableResponse(
        int requestHash,
        String newEtag)
    {
        DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
        return cacheEntry != null &&
               cacheEntry.etag() != null &&
               cacheEntry.etag().equals(newEtag) &&
               cacheEntry.recentAuthorizationHeader() != null;
    }

    public boolean matchCacheableRequest(
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope,
        int requestHash)
    {
        boolean canHandleRequest = false;
        final DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);

        if (satisfiedByCache(requestHeaders) &&
            cacheEntry != null &&
            cacheEntry.canServeRequest(requestHeaders, authScope))
        {
            canHandleRequest = true;
        }
        return canHandleRequest;
    }


    public void purge(
        int requestHash)
    {
        DefaultCacheEntry cacheEntry = this.cachedEntries.remove(requestHash);
        if (cacheEntry != null)
        {
            cacheEntry.purge();
        }
    }


    public boolean isUpdatedByEtagToRetry(
        ListFW<HttpHeaderFW> requestHeaders,
        String ifNoneMatch,
        DefaultCacheEntry cacheEntry)
    {
        ListFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();
        if (isPreferWait(requestHeaders)
            && !isPreferenceApplied(responseHeaders))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String newEtag = cacheEntry.etag();
            assert status != null;

            if (ifNoneMatch != null && newEtag != null)
            {
                return !(status.equals(HttpStatus.OK_200) && ifNoneMatch.equals(newEtag));
            }
        }

        return true;
    }

    public boolean isUpdatedByResponseHeadersToRetry(
        ListFW<HttpHeaderFW> requestHeaders,
        ListFW<HttpHeaderFW> responseHeaders,
        String ifNoneMatch,
        int requestHash)
    {

        if (isPreferWait(requestHeaders)
            && !isPreferenceApplied(responseHeaders))
        {
            String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
            String newEtag = getHeader(responseHeaders, ETAG);
            boolean etagMatches = false;
            assert status != null;

            if (ifNoneMatch != null && newEtag != null)
            {
                etagMatches = status.equals(HttpStatus.OK_200) && ifNoneMatch.equals(newEtag);

                if (etagMatches)
                {
                    DefaultCacheEntry cacheEntry = cachedEntries.get(requestHash);
                    cacheEntry.updateResponseHeader(responseHeaders);
                }
            }

            boolean notModified = status.equals(NOT_MODIFIED_304) || etagMatches;

            return !notModified;
        }

        return true;
    }

    public void send304(
        DefaultCacheEntry entry,
        ListFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                              currentTimeMillis(), acceptReplyId, "304");
        }

        if (isPreferIfNoneMatch(requestHeaders))
        {
            String preferWait = getHeader(requestHeaders, PREFER);
            String ifNonMatch = getHeader(requestHeaders, IF_NONE_MATCH);
            writer.doHttpResponse(acceptReply,
                                          acceptRouteId,
                                          acceptReplyId,
                                          supplyTrace.getAsLong(),
                                          e -> e.item(h -> h.name(STATUS).value(NOT_MODIFIED_304))
                                                .item(h -> h.name(ETAG).value(ifNonMatch))
                                                .item(h -> h.name(PREFERENCE_APPLIED).value(preferWait)));
        }
        else
        {
            writer.doHttpResponse(acceptReply,
                                          acceptRouteId,
                                          acceptReplyId,
                                          supplyTrace.getAsLong(),
                                          e -> e.item(h -> h.name(STATUS).value("304"))
                                                .item(h -> h.name(ETAG).value(entry.etag())));
        }
        // count all responses
        counters.responses.getAsLong();
    }

    private void updateCache(
        int requestHash,
        DefaultCacheEntry cacheEntry)
    {
        cachedEntries.put(requestHash, cacheEntry);
    }

}
