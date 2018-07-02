/**
 * Copyright 2016-2017 The Reaktivity Project
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

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.InitialRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request.Type;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Cache
{

    final Writer writer;
    final BudgetManager budgetManager;
    final Int2CacheHashMapWithLRUEviction cachedEntries;
    final BufferPool cachedResponseBufferPool;
    final BufferPool responseBufferPool;
    final BufferPool refreshBufferPool;
    final BufferPool subscriberBufferPool;
    final BufferPool request1BufferPool;
    final BufferPool request2BufferPool;
    final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> request2HeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();
    final WindowFW windowRO = new WindowFW();

    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();
    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final Supplier<String> etagSupplier;
    final Long2ObjectHashMap<PendingCacheEntries> uncommittedRequests = new Long2ObjectHashMap<>();

    public Cache(
            LongObjectBiConsumer<Runnable> scheduler,
            BudgetManager budgetManager,
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool,
            Long2ObjectHashMap<Request> correlations,
            Supplier<String> etagSupplier,
            Function<String, LongSupplier> supplyCounter,
            LongConsumer entryCount)
    {
        this.scheduler = scheduler;
        this.budgetManager = budgetManager;
        this.correlations = correlations;
        this.writer = new Writer(writeBuffer);
        this.refreshBufferPool = new CountingBufferPool(
                bufferPool.duplicate(),
                supplyCounter.apply("refresh.request.acquires"),
                supplyCounter.apply("refresh.request.releases"));
        this.request1BufferPool = bufferPool.duplicate();
        this.request2BufferPool = bufferPool.duplicate();

        this.cachedResponseBufferPool = bufferPool.duplicate();
        this.responseBufferPool = bufferPool.duplicate();
        this.subscriberBufferPool = bufferPool.duplicate();
        this.cachedEntries = new Int2CacheHashMapWithLRUEviction(entryCount);
        this.etagSupplier = etagSupplier;
    }

    public void put(
        int requestUrlHash,
        CacheableRequest request)
    {
        CacheEntry oldCacheEntry = cachedEntries.get(requestUrlHash);
        if (oldCacheEntry == null)
        {
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    true);
            updateCache(requestUrlHash, cacheEntry);
        }
        else
        {
            boolean expectSubscribers = request.getType() == Type.INITIAL_REQUEST ? true: oldCacheEntry.expectSubscribers();
            CacheEntry cacheEntry = new CacheEntry(
                    this,
                    request,
                    expectSubscribers);

            if (cacheEntry.isIntendedForSingleUser())
            {
                cacheEntry.purge();
            }
            else if (oldCacheEntry.isUpdatedBy(request))
            {
                updateCache(requestUrlHash, cacheEntry);

                boolean notVaries = oldCacheEntry.doesNotVaryBy(cacheEntry);
                if (notVaries)
                {
                    oldCacheEntry.subscribers(cacheEntry::serveClient);
                }
                else
                {
                    oldCacheEntry.subscribers(subscriber ->
                    {
                        final MessageConsumer acceptReply = subscriber.acceptReply();
                        final long acceptReplyStreamId = subscriber.acceptReplyStreamId();
                        final long acceptCorrelationId = subscriber.acceptCorrelationId();
                        this.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
                    });
                }

                oldCacheEntry.purge();
            }
            else
            {
                cacheEntry.purge();
                if (request.getType() == Request.Type.CACHE_REFRESH)
                {
                    oldCacheEntry.refresh(request);
                }
            }
        }
    }

    private void updateCache(
            int requestUrlHash,
            CacheEntry cacheEntry)
    {
        cacheEntry.commit();
        cachedEntries.put(requestUrlHash, cacheEntry);
        PendingCacheEntries result = this.uncommittedRequests.remove(requestUrlHash);
        if (result != null)
        {
            result.addSubscribers(cacheEntry);
        }
    }

    public boolean handleInitialRequest(
            int requestURLHash,
            ListFW<HttpHeaderFW> request,
            short authScope,
            CacheableRequest cacheableRequest)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        if (cacheEntry != null)
        {
            return serveRequest(cacheEntry, request, authScope, cacheableRequest);
        }
        else
        {
            return false;
        }
    }

    public void handleOnUpdateRequest(
            int requestURLHash,
            OnUpdateRequest onUpdateRequest,
            ListFW<HttpHeaderFW> requestHeaders,
            short authScope)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        PendingCacheEntries uncommittedRequest = this.uncommittedRequests.get(requestURLHash);
        assert cacheEntry != null && uncommittedRequest == null || uncommittedRequest != null && cacheEntry == null;

        String ifNoneMatch = HttpHeadersUtil.getHeader(requestHeaders, HttpHeaders.IF_NONE_MATCH);
        assert ifNoneMatch != null;
        if (uncommittedRequest != null && ifNoneMatch.contains(uncommittedRequest.etag())
                && doesNotVary(requestHeaders, uncommittedRequest.request))
        {
            uncommittedRequest.subscribe(onUpdateRequest);
        }
        else if (cacheEntry == null)
        {
            final MessageConsumer acceptReply = onUpdateRequest.acceptReply();
            final long acceptReplyStreamId = onUpdateRequest.acceptReplyStreamId();
            final long acceptCorrelationId = onUpdateRequest.acceptCorrelationId();
            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
        }
        else if (cacheEntry.isUpdateRequestForThisEntry(requestHeaders))
        {
            // TODO return value ??
            cacheEntry.subscribeToUpdate(onUpdateRequest);
        }
        else if (cacheEntry.canServeUpdateRequest(requestHeaders))
        {
            cacheEntry.serveClient(onUpdateRequest);
        }
        else
        {
            final MessageConsumer acceptReply = onUpdateRequest.acceptReply();
            final long acceptReplyStreamId = onUpdateRequest.acceptReplyStreamId();
            final long acceptCorrelationId = onUpdateRequest.acceptCorrelationId();
            writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
        }
    }

    private boolean doesNotVary(
            ListFW<HttpHeaderFW> requestHeaders,
            InitialRequest request)
    {
        ListFW<HttpHeaderFW> cachedRequestHeaders = request.getRequestHeaders(cachedRequestHeadersRO);
        ListFW<HttpHeaderFW> cachedResponseHeaders = request.getResponseHeaders(cachedResponseHeadersRO);
        return CacheUtils.doesNotVary(requestHeaders, cachedResponseHeaders, cachedRequestHeaders);
    }

    private boolean serveRequest(
            CacheEntry entry,
            ListFW<HttpHeaderFW> request,
            short authScope,
            AnswerableByCacheRequest cacheableRequest)
    {
        if (entry.canServeRequest(request, authScope))
        {
            entry.serveClient(cacheableRequest);
            return true;
        }
        return false;
    }

    private boolean serveUpdateRequest(
            CacheEntry entry,
            ListFW<HttpHeaderFW> request,
            OnUpdateRequest updateRequest)
    {
        if (entry.canServeUpdateRequest(request))
        {
            entry.serveClient(updateRequest);
            return true;
        }
        return false;
    }

    public void notifyUncommitted(InitialRequest request)
    {
        this.uncommittedRequests.computeIfAbsent(request.requestURLHash(), p -> new PendingCacheEntries(request));
    }

    public void purge(CacheEntry entry)
    {
        this.cachedEntries.remove(entry.requestUrl());
        entry.purge();
    }

    public void purgeOld()
    {
        this.cachedEntries.purgeLRU();
    }

}
