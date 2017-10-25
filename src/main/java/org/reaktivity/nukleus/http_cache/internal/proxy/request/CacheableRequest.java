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

    private final BufferPool bufferPool;
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
        BufferPool bufferPool,
        int requestSlot,
        int requestSize, RouteManager router)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
        this.bufferPool = bufferPool;
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

    public ListFW<HttpHeaderFW> getRequestHeaders()
    {
        // TODO Auto-generated method stub
//        return null;
        throw new RuntimeException("DPW not implemented");
    }

    public void cache(ListFW<HttpHeaderFW> responseHeaders)
    {
        // DPW TODO abort if not cacheable
        setupResponseBuffer();
        MutableDirectBuffer buffer = bufferPool.buffer(responseSlot);
        final int headersSize = responseHeaders.sizeof();
        buffer.putBytes(responseSize, responseHeaders.buffer(), responseHeaders.offset(), headersSize);
        responseSize += headersSize;
        this.responseHeadersSize = headersSize;
    }

    private void setupResponseBuffer()
    {
        this.responseSlot = bufferPool.acquire(acceptReplyStreamId());
        this.responseHeadersSize = 0;
        this.responseSize = 0;
    }

    public void cache(DataFW data)
    {
        OctetsFW payload = data.payload();
        int sizeof = payload.sizeof();
        MutableDirectBuffer buffer = bufferPool.buffer(responseSlot);
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
        bufferPool.release(requestSlot);
        bufferPool.release(responseSlot);
    }

    @Override
    public void complete()
    {
        if (!cachingResponse)
        {
            bufferPool.release(requestSlot);
            bufferPool.release(responseSlot);
        }
    }
}
