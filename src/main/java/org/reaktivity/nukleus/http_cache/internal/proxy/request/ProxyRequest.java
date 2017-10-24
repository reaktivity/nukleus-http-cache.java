package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.function.MessageConsumer;

public class ProxyRequest extends Request
{

    public ProxyRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId);
    }

    @Override
    public Type getType()
    {
        return Type.PROXY;
    }

    @Override
    public void abort()
    {
        // NOOP
    }

    @Override
    public void complete()
    {
        // NOOP
    }
}
