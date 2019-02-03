/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

import java.util.Objects;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeaderOrDefault;

public class CacheRefreshRequest extends CacheableRequest
{
    private final CacheEntry updatingEntry;
    private final Cache cache;

    public CacheRefreshRequest(
            CacheableRequest req,
            BufferPool bufferPool,
            int requestSlot,
            String etag,
            CacheEntry cacheEntry,
            Cache cache)
    {
        // TODO eliminate reference /GC duplication (Flyweight pattern?)
        super(req.acceptReply,
              req.acceptRouteId,
              req.acceptReplyStreamId,
              req.acceptCorrelationId,
              req.connectRouteId,
              req.supplyCorrelationId,
              req.supplyInitialId,
              req.supplyReceiver,
              req.requestURLHash(),
              bufferPool,
              requestSlot,
              req.router,
              req.authorizationHeader(),
              req.authorization(),
              req.authScope(),
              etag);
        this.updatingEntry = cacheEntry;
        this.cache = cache;
    }

    @Override
    public boolean storeResponseHeaders(
        ListFW<HttpHeaderFW> responseHeaders,
        Cache cache,
        BufferPool bufferPool)
    {
        if (responseHeaders.anyMatch(h ->
                (":status".equals(h.name().asString()) &&
                        (Objects.requireNonNull(h.value().asString()).startsWith("2") ||
                                Objects.requireNonNull(h.value().asString()).equals("304")))))
        {
            if(responseHeaders.anyMatch(h ->
                    (":status".equals(h.name().asString())
                            && Objects.requireNonNull(h.value().asString()).startsWith("2"))))
            {
                etag(getHeaderOrDefault(responseHeaders, ETAG, cache.getEtagSupplier().get()));
            }

            boolean noError = super.storeResponseHeaders(responseHeaders, cache, bufferPool);
            if (!noError)
            {
                this.purge();
            }
            return noError;
        }
        else
        {
            this.purge();
            return false;
        }
}

    @Override
    public Type getType()
    {
        return Type.CACHE_REFRESH;
    }

    @Override
    public void purge()
    {
        if (this.state != CacheState.COMMITTED)
        {
            this.cache.purge(updatingEntry);
        }
        super.purge();
    }
}
