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
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

import java.util.Objects;

public class CacheRefreshRequest extends CacheableRequest
{
    private final DefaultCacheEntry updatingEntry;
    private final DefaultCache cache;

    public CacheRefreshRequest(
            CacheableRequest req,
            BufferPool bufferPool,
            int requestSlot,
            String etag,
            DefaultCacheEntry cacheEntry,
            DefaultCache cache)
    {
        // TODO eliminate reference /GC duplication (Flyweight pattern?)
        super(req.acceptReply,
              req.acceptRouteId,
              req.acceptReplyStreamId,
              req.connectRouteId,
              req.supplyInitialId,
              req.supplyReplyId,
              req.supplyReceiver,
              req.requestURLHash(),
              bufferPool,
              requestSlot,
              req.router,
              req.authorizationHeader(),
              req.authorization(),
              req.authScope(),
              etag,
              true);
        this.updatingEntry = cacheEntry;
        this.cache = cache;
    }

    @Override
    public boolean storeResponseHeaders(
        ListFW<HttpHeaderFW> responseHeaders,
        DefaultCache cache,
        BufferPool bufferPool)
    {
        if (responseHeaders.anyMatch(h ->
                (":status".equals(h.name().asString()) &&
                        (Objects.requireNonNull(h.value().asString()).startsWith("2") ||
                                Objects.requireNonNull(h.value().asString()).equals("304")))))
        {

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
