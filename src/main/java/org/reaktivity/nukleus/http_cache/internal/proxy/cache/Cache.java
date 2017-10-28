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

import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;

public class Cache
{

    final Writer writer;
    final Long2ObjectHashMap<CacheEntry> cachedEntries;
    final BufferPool cachedRequestBufferPool;
    final BufferPool cachedResponseBufferPool;
    final BufferPool responseBufferPool;
    final BufferPool requestBufferPool;
    final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> cachedResponseHeadersRO = new HttpBeginExFW().headers();
    final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();
    final WindowFW windowRO = new WindowFW();

    static final String RESPONSE_IS_STALE = "110 - \"Response is Stale\"";

    final CacheControl responseCacheControlFW = new CacheControl();
    final CacheControl cachedRequestCacheControlFW = new CacheControl();
    final CacheControl requestCacheControlFW = new CacheControl();
    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Request> correlations;
    final Supplier<String> etagSupplier;

    public Cache(
            LongObjectBiConsumer<Runnable> scheduler,
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool,
            Long2ObjectHashMap<Request> correlations,
            RouteManager router,
            Supplier<String> etagSupplier)
    {
        this.scheduler = scheduler;
        this.correlations = correlations;
        this.writer = new Writer(writeBuffer);
        this.cachedRequestBufferPool = bufferPool;
        this.requestBufferPool = bufferPool;
        this.cachedResponseBufferPool = bufferPool.duplicate();
        this.responseBufferPool = bufferPool.duplicate();
        this.cachedEntries = new Long2ObjectHashMap<>();
        this.etagSupplier = etagSupplier;
    }

    public void put(
        int requestUrlHash,
        CacheableRequest request)
    {
        CacheEntry cacheEntry = new CacheEntry(
                this,
                request);

        if (cacheEntry.isIntendedForSingleUser())
        {
            cacheEntry.cleanUp();
            return;
        }

        CacheEntry oldCacheEntry = cachedEntries.get(requestUrlHash);
        if (oldCacheEntry == null)
        {
            cachedEntries.put(requestUrlHash, cacheEntry);
            return;
        }
        else if (oldCacheEntry.isUpdateBy(request))
        {
            cachedEntries.put(requestUrlHash, cacheEntry);
            oldCacheEntry.subscribersOnUpdate().forEach(r ->
            {
                oldCacheEntry.cleanUp();
                if (!this.serveRequest(
                        cacheEntry,
                        r.getRequestHeaders(cachedRequestHeadersRO),
                        r.authScope(),
                        r))
                {
                    throw new RuntimeException("Not implemented, probably better" +
                            " to cancel push promise, but maybe should forward request?");
                }
            });
        }
        else
        {
            cacheEntry.cleanUp();
            if (request.getType().equals(Request.Type.CACHE_REFRESH))
            {
                oldCacheEntry.refresh(request);
            }
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

    public boolean handleOnUpdateRequest(
            int requestURLHash,
            OnUpdateRequest onModificationRequest,
            ListFW<HttpHeaderFW> requestHeaders,
            short authScope)
    {
        final CacheEntry cacheEntry = cachedEntries.get(requestURLHash);
        if (cacheEntry == null)
        {
            return false;
        }
        else if (cacheEntry.isMatch(requestHeaders))
        {
            return cacheEntry.subscribeToUpdate(onModificationRequest);
        }
        else
        {
            cacheEntry.serveClient(onModificationRequest);
            return true;
        }
    }

    private boolean serveRequest(
            CacheEntry entry,
            ListFW<HttpHeaderFW> request,
            short authScope,
            CacheableRequest cacheableRequest)
    {
        if (entry.canServeRequest(request, authScope))
        {
            entry.serveClient(cacheableRequest);
            return true;
        }
        return false;
    }

}
