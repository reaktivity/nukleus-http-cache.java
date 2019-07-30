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

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.route.RouteManager;

import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

public class DefaultRequest extends Request
{
    private long acceptReplyId;
    private long connectRouteId;
    private final int requestURLHash;
    private final long authorization;
    private final short authScope;
    private BufferPool bufferPool;
    private int requestSlot;
    private final boolean authorizationHeader;
    private String etag;
    final LongFunction<MessageConsumer> supplyReceiver;
    private MessageConsumer signaler;
    private int attempts;
    final LongUnaryOperator supplyReplyId;
    final LongUnaryOperator supplyInitialId;

    public DefaultRequest(
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long acceptReplyId,
        long connectRouteId,
        RouteManager router,
        LongFunction<MessageConsumer> supplyReceiver,
        int requestURLHash,
        BufferPool bufferPool,
        int requestSlot,
        boolean authorizationHeader,
        long authorization,
        short authScope,
        String etag,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        boolean isEmulated)
    {
        super(acceptReply, acceptRouteId, acceptReplyId, router, isEmulated);
        this.acceptReplyId = acceptReplyId;
        this.connectRouteId = connectRouteId;
        this.requestURLHash = requestURLHash;
        this.bufferPool = bufferPool;
        this.requestSlot = requestSlot;
        this.authorizationHeader = authorizationHeader;
        this.authorization = authorization;
        this.authScope = authScope;
        this.etag = etag;
        this.supplyReceiver = supplyReceiver;
        this.signaler = supplyReceiver.apply(acceptStreamId);
        this.supplyReplyId = supplyReplyId;
        this.supplyInitialId = supplyInitialId;
    }

    @Override
    public Type getType()
    {
        return Type.CACHEABLE_REQUEST;
    }

    public final boolean authorizationHeader()
    {
        return authorizationHeader;
    }

    public final long authorization()
    {
        return authorization;
    }

    public final short authScope()
    {
        return authScope;
    }

    public final String etag()
    {
        return etag;
    }

    public final int requestURLHash()
    {
        return requestURLHash;
    }

    public void etag(String etag)
    {
        this.etag = etag;
    }

    public void purge()
    {
        if (requestSlot != Slab.NO_SLOT)
        {
            bufferPool.release(requestSlot);
            this.requestSlot = Slab.NO_SLOT;
        }
    }

    public MessageConsumer getSignaler()
    {
        return signaler;
    }

    public void incAttempts()
    {
        attempts++;
    }

    public int attempts()
    {
        return attempts;
    }

    public final ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO)
    {
        return getRequestHeaders(requestHeadersRO, bufferPool);
    }

    public final ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO,
        BufferPool bp)
    {
        final MutableDirectBuffer buffer = bp.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    public long connectRouteId()
    {
        return connectRouteId;
    }

    public long getAcceptReplyId()
    {
        return acceptReplyId;
    }

    public LongUnaryOperator supplyReplyId()
    {
        return supplyReplyId;
    }

    public LongUnaryOperator supplyInitialId()
    {
        return supplyInitialId;
    }

    public LongFunction<MessageConsumer> supplyReceiver()
    {
        return supplyReceiver;
    }
}
