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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.DefaultRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

public class DefaultCache
{
    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();

    public final BufferPool cachedRequestBufferPool;
    public final BufferPool cachedResponseBufferPool;

    final Writer writer;
    final BudgetManager budgetManager;
    final Int2CacheHashMapWithLRUEviction cachedEntries;

    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;

    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final LongSupplier supplyTrace;
    final Int2ObjectHashMap<PendingInitialRequests> pendingInitialRequestsMap = new Int2ObjectHashMap<>();
    final HttpCacheCounters counters;
    final SignalingExecutor executor;

    public DefaultCache(
        LongObjectBiConsumer<Runnable> scheduler,
        BudgetManager budgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        BufferPool cacheBufferPool,
        Long2ObjectHashMap<Request> correlations,
        HttpCacheCounters counters,
        LongConsumer entryCount,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor)
    {
        this.scheduler = scheduler;
        this.budgetManager = budgetManager;
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
        this.requestBufferPool = requestBufferPool.duplicate();
        this.responseBufferPool = requestBufferPool.duplicate();
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.counters = counters;
        this.supplyTrace = requireNonNull(supplyTrace);
        this.executor = executor;
    }

    public DefaultCacheEntry get(int requestUrlHash)
    {
        return cachedEntries.get(requestUrlHash);
    }

    public DefaultCacheEntry put(int requestUrlHash)
    {
        DefaultCacheEntry oldCacheEntry = cachedEntries.get(requestUrlHash);
        if (oldCacheEntry == null)
        {
            DefaultCacheEntry cacheEntry = new DefaultCacheEntry(
                    this,
                    requestUrlHash,
                    cachedRequestBufferPool);
            updateCache(requestUrlHash, cacheEntry);
            return cacheEntry;
        }
        else
        {
            return oldCacheEntry;
        }
    }

    public void signalForUpdatedCacheEntry(int requestUrlHash)
    {
       pendingInitialRequestsMap.forEach((k, request) ->
       {
           DefaultRequest defaultRequest = request.initialRequest();
           if(defaultRequest.requestURLHash() == requestUrlHash)
           {
               writer.doSignal(defaultRequest.getSignaler(),
                   defaultRequest.acceptRouteId,
                   defaultRequest.acceptReplyStreamId,
                   supplyTrace.getAsLong(),
                   CACHE_ENTRY_UPDATED_SIGNAL);
           }
       });
    }

    private void updateCache(
            int requestUrlHash,
            DefaultCacheEntry cacheEntry)
    {
        cachedEntries.put(requestUrlHash, cacheEntry);
    }

    public boolean handleCacheableRequest(
        int requestURLHash,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope,
        DefaultRequest defaultRequest)
    {
        final DefaultCacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        if (cacheEntry != null)
        {
            return serveRequest(cacheEntry, requestHeaders, authScope, defaultRequest);
        }
        else
        {
            return false;
        }
    }

    public void sendPendingInitialRequests(int requestURLHash)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.remove(requestURLHash);
        if (pendingInitialRequests != null)
        {
            final PendingInitialRequests newPendingInitialRequests = pendingInitialRequests.withNextInitialRequest();
            if (newPendingInitialRequests != null)
            {
                pendingInitialRequestsMap.put(requestURLHash, newPendingInitialRequests);
                sendPendingInitialRequest(newPendingInitialRequests.initialRequest());
            }
        }
    }

    private void sendPendingInitialRequest(
        final DefaultRequest request)
    {
        long connectRouteId = request.connectRouteId();
        long connectInitialId = request.supplyInitialId().applyAsLong(connectRouteId);
        MessageConsumer connectInitial = request.supplyReceiver().apply(connectInitialId);
        long connectReplyId = request.supplyReplyId().applyAsLong(connectInitialId);
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(requestHeadersRO);

        correlations.put(connectReplyId, request);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent pending request]\n",
                    currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        writer.doHttpRequest(connectInitial, connectRouteId, connectInitialId, supplyTrace.getAsLong(),
                builder -> requestHeaders.forEach(h ->  builder.item(item -> item.name(h.name()).value(h.value()))));
        writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, supplyTrace.getAsLong());
    }

    public boolean hasPendingInitialRequests(
        int requestURLHash)
    {
        return pendingInitialRequestsMap.containsKey(requestURLHash);
    }

    public void addPendingRequest(
        DefaultRequest initialRequest)
    {
        PendingInitialRequests pendingInitialRequests = pendingInitialRequestsMap.get(initialRequest.requestURLHash());
        pendingInitialRequests.subscribe(initialRequest);
    }

    public void createPendingInitialRequests(
        DefaultRequest initialRequest)
    {
        pendingInitialRequestsMap.put(initialRequest.requestURLHash(), new PendingInitialRequests(initialRequest));
    }

    private boolean doesNotVary(
        ListFW<HttpHeaderFW> requestHeaders,
        DefaultRequest request)
    {
//        ListFW<HttpHeaderFW> cachedRequestHeaders = request.getRequestHeaders(cachedRequestHeadersRO);
//        ListFW<HttpHeaderFW> cachedResponseHeaders = request.getResponseHeaders(cachedResponseHeadersRO);
//        return CacheUtils.doesNotVary(requestHeaders, cachedResponseHeaders, cachedRequestHeaders);
        return true;
    }

    private boolean serveRequest(
        DefaultCacheEntry entry,
        ListFW<HttpHeaderFW> requestHeaders,
        short authScope,
        DefaultRequest defaultRequest)
    {
        if (entry.canServeRequest(requestHeaders, authScope))
        {
            final String requestAuthorizationHeader = getHeader(requestHeaders, AUTHORIZATION);
            entry.recentAuthorizationHeader(requestAuthorizationHeader);

            boolean etagMatched = CacheUtils.isMatchByEtag(requestHeaders, entry.getEtag());
            if (etagMatched)
            {
                send304(entry, defaultRequest);
            }
            else
            {
                signalForUpdatedCacheEntry(defaultRequest.requestURLHash());
            }

            return true;
        }
        return false;
    }

    private void send304(
        DefaultCacheEntry entry,
        DefaultRequest request)
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                    currentTimeMillis(), request.acceptReplyId(), "304");
        }

        writer.doHttpResponse(request.acceptReply, request.acceptRouteId,
                request.acceptReplyId(), supplyTrace.getAsLong(), e -> e.item(h -> h.name(STATUS).value("304"))
                      .item(h -> h.name(ETAG).value(entry.getEtag())));
        writer.doHttpEnd(request.acceptReply, request.acceptRouteId, request.acceptReplyId(), supplyTrace.getAsLong());

        // count all responses
        counters.responses.getAsLong();
    }

    private static int copy(int fromSlot, BufferPool fromBP, BufferPool toBP)
    {
        int toSlot = toBP.acquire(0);
        // should we purge old cache entries ?
        if (toSlot != NO_SLOT)
        {
            DirectBuffer fromBuffer = fromBP.buffer(fromSlot);
            MutableDirectBuffer toBuffer = toBP.buffer(toSlot);
            toBuffer.putBytes(0, fromBuffer, 0, fromBuffer.capacity());
        }

        return toSlot;
    }


    public void purge(
        DefaultCacheEntry entry)
    {
        this.cachedEntries.remove(entry.requestURLHash());
        entry.purge();
    }

    public boolean purgeOld()
    {
        return this.cachedEntries.purgeLRU();
    }

}
