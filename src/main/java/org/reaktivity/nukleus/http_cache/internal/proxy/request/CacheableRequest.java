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

import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.route.RouteManager;

public class CacheableRequest extends Request
{

    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;
    final int requestSlot;
    final int requestSize;
    final int requestURLHash;
    int responseSlot = Slab.NO_SLOT;
    int responseHeadersSize;
    int responseSize;
    boolean cachingResponse;    // TODO, consider using state management via method references
    final short authScope;
    final MessageConsumer connect;
    final String connectName;
    final long connectRef;
    final LongSupplier supplyCorrelationId;
    final LongSupplier supplyStreamId;

    public CacheableRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        MessageConsumer connect,
        String connectName,
        long connectRef,
        LongSupplier supplyCorrelationId,
        LongSupplier supplyStreamId,
        int requestURLHash,
        BufferPool responseBufferPool,
        BufferPool requestBufferPool,
        int requestSlot,
        int requestSize,
        RouteManager router,
        short authScope)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
        this.requestBufferPool = requestBufferPool;
        this.responseBufferPool = responseBufferPool;
        this.requestSlot = requestSlot;
        this.requestSize = requestSize;
        this.requestURLHash = requestURLHash;
        this.cachingResponse = true;
        this.authScope = authScope;
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyStreamId = supplyStreamId;

        this.connect = connect;
        this.connectName = connectName;
        this.connectRef = connectRef;
    }

    @Override
    public Type getType()
    {
        return Type.CACHEABLE;
    }

    public ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> requestHeadersRO)
    {
        final MutableDirectBuffer buffer = requestBufferPool.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, requestSize);
    }

    // TODO remove need for
    public void copyRequestTo(MutableDirectBuffer buffer)
    {
        MutableDirectBuffer requestBuffer = requestBufferPool.buffer(requestSlot);
        requestBuffer.getBytes(0, buffer, 0, requestSize);
    }

    public void cache(ListFW<HttpHeaderFW> responseHeaders)
    {
        // DPW TODO abort if not cacheable
        setupResponseBuffer();
        MutableDirectBuffer buffer = responseBufferPool.buffer(responseSlot);
        final int headersSize = responseHeaders.sizeof();
        buffer.putBytes(responseSize, responseHeaders.buffer(), responseHeaders.offset(), headersSize);
        responseSize += headersSize;
        this.responseHeadersSize = headersSize;
    }

    private void setupResponseBuffer()
    {
        this.responseSlot = responseBufferPool.acquire(acceptReplyStreamId());
        this.responseHeadersSize = 0;
        this.responseSize = 0;
    }

    public void cache(DataFW data)
    {
        OctetsFW payload = data.payload();
        int sizeof = payload.sizeof();
        MutableDirectBuffer buffer = responseBufferPool.buffer(responseSlot);
        buffer.putBytes(responseSize, payload.buffer(), payload.offset(), sizeof);
        responseSize += sizeof;
    }

    public void cache(EndFW end, Cache cache)
    {
        cache.put(requestURLHash(), this);
    }

    private int requestURLHash()
    {
        return this.requestURLHash;
    }

    @Override
    public void abort()
    {
        requestBufferPool.release(requestSlot);
        if (responseSlot != Slab.NO_SLOT)
        {
            responseBufferPool.release(responseSlot);
        }
    }

    @Override
    public void complete()
    {
        if (!cachingResponse)
        {
            requestBufferPool.release(requestSlot);
            responseBufferPool.release(responseSlot);
        }// else cache's responsibility to clear, TODO clean up this abstraction
    }

    public short authScope()
    {
        return authScope;
    }

    public String connectName()
    {
        return connectName;
    }

    public long connectRef()
    {
        return connectRef;
    }

    public LongSupplier supplyCorrelationId()
    {
        return supplyCorrelationId;
    }

    public LongSupplier supplyStreamId()
    {
        return supplyStreamId;
    }

    // TODO hide abstraction
    public int responseSlot()
    {
        return responseSlot;
    }

    // TODO hide abstraction
    public int responseHeadersSize()
    {
        return responseHeadersSize;
    }

    // TODO hide abstraction
    public int responseSize()
    {
        return responseSize;
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO)
    {
        MutableDirectBuffer responseBuffer = responseBufferPool.buffer(responseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public MessageConsumer connect()
    {
        return connect;
    }
}
