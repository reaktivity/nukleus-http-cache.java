package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public class OnModification extends CacheableRequest
{

    public OnModification(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        BufferPool bufferPool,
        int requestSlot,
        int requestSize,
        RouteManager router)
    {
        super(
            acceptName,
            acceptReply,
            acceptReplyStreamId,
            acceptCorrelationId,
            bufferPool,
            requestSlot,
            requestSize,
            router);
    }

    @Override
    public Type getType()
    {
        return Type.ON_MODIFIED;
    }

}
