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
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class CacheRefreshRequest extends CacheableRequest
{

    public CacheRefreshRequest(
            CacheableRequest req,
            int requestSlot,
            String etag,
            CacheEntry cacheEntry)
    {
        // TODO eliminate reference /GC duplication (Flyweight pattern?)
        super(req.acceptName,
              req.acceptReply,
              req.acceptReplyStreamId,
              req.acceptCorrelationId,
              req.connect,
              req.connectRef,
              req.supplyCorrelationId,
              req.supplyStreamId,
              req.requestURLHash(),
              req.responseBufferPool,
              req.requestBufferPool(),
              requestSlot,
              req.requestSize(),
              req.router,
              req.authScope(),
              etag);
        cacheEntry(cacheEntry);
    }

    public void cache(
        ListFW<HttpHeaderFW> responseHeaders,
        Cache cache)
    {
        if (responseHeaders.anyMatch(h ->
                ":status".equals(h.name().asString()) &&
                "200".equals(h.value().asString())))
        {
            super.cache(responseHeaders, cache);
        }
        else
        {
            this.purge();
        }
}

    @Override
    public Type getType()
    {
        return Type.CACHE_REFRESH;
    }

}
