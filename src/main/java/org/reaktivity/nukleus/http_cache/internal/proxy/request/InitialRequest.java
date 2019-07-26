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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.route.RouteManager;

import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public class InitialRequest extends CacheableRequest
{
    private final DefaultCache cache;

    public InitialRequest(
            DefaultCache cache,
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptStreamId,
            long acceptReplyStreamId,
            long connectRouteId,
            LongUnaryOperator supplyInitialId,
            LongUnaryOperator supplyReplyId,
            LongFunction<MessageConsumer> supplyReceiver,
            int requestURLHash,
            BufferPool bufferPool,
            int requestSlot,
            RouteManager router,
            boolean authorizationHeader,
            long authorization,
            short authScope,
            String etag,
            boolean isEmulated)
    {
        super(acceptReply,
              acceptRouteId,
              acceptStreamId,
              acceptReplyStreamId,
              connectRouteId,
              supplyInitialId,
              supplyReplyId,
              supplyReceiver,
              requestURLHash,
              bufferPool,
              requestSlot,
              router,
              authorizationHeader,
              authorization,
              authScope,
              etag,
              isEmulated);
        this.cache = cache;
    }

    @Override
    public Type getType()
    {
        return Type.INITIAL_REQUEST;
    }

    @Override
    public boolean storeResponseHeaders(
            ListFW<HttpHeaderFW> responseHeaders,
            DefaultCache cache,
            BufferPool bp)
    {
        return super.storeResponseHeaders(responseHeaders, cache, bp);
    }

    public void purge()
    {
        super.purge();
        cache.sendPendingInitialRequests(requestURLHash());
    }

    @Override
    public boolean cache(EndFW end, DefaultCache cache)
    {
        boolean cached = super.cache(end, cache);
        cache.servePendingInitialRequests(requestURLHash());
        return cached;
    }

}
