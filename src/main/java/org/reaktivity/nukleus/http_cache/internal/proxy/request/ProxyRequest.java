package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public class ProxyRequest extends Request
{

    public ProxyRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        RouteManager router)
    {
        super(acceptName, acceptReply, acceptReplyStreamId, acceptCorrelationId, router);
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
