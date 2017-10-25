package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.route.RouteManager;

public class CacheableRequest extends Request
{

    private final BufferPool requestBufferPool;
    private final BufferPool responseBufferPool;
    private final int requestSlot;
    private final int requestSize;
    private final int requestUrlHash;
    private int responseSlot;
    private int responseHeadersSize;
    private int responseSize;
    private boolean cachingResponse;    // TODO, move to state

    public CacheableRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        int requestURLHash,
        BufferPool responseBufferPool,
        BufferPool requestBufferPool,
        int requestSlot,
        int requestSize,
        RouteManager router)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
        this.requestBufferPool = requestBufferPool;
        this.responseBufferPool = responseBufferPool;
        this.requestSlot = requestSlot;
        this.requestSize = requestSize;
        this.requestUrlHash = requestURLHash;
        this.cachingResponse = true;
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
        cache.put(requestUrlHash(), requestSlot, requestSize, responseSlot, responseHeadersSize, responseSize);
    }

    private int requestUrlHash()
    {
        return this.requestUrlHash;
    }

    @Override
    public void abort()
    {
        requestBufferPool.release(requestSlot);
        responseBufferPool.release(responseSlot);
    }

    @Override
    public void complete()
    {
        if (!cachingResponse)
        {
            requestBufferPool.release(requestSlot);
            responseBufferPool.release(responseSlot);
        }
    }
}
