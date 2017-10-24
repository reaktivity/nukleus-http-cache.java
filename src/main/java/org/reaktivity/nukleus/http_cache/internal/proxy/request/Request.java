package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class Request
{
    public enum Type
    {
        ON_MODIFIED, PROXY, CACHEABLE
    }

    private final String acceptName;
    private final MessageConsumer acceptReply;
    private final long acceptReplyStreamId;
    private final long acceptCorrelationId;
    private final RouteManager router;

    public Request(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        RouteManager router)
    {
        this.acceptName = acceptName;
        this.acceptReply = acceptReply;
        this.acceptReplyStreamId = acceptReplyStreamId;
        this.acceptCorrelationId = acceptCorrelationId;
        this.router = router;
    }

    public abstract Type getType();

    public String acceptName()
    {
        return acceptName;
    }

    public MessageConsumer acceptReply()
    {
        return acceptReply;
    }

    public long acceptReplyStreamId()
    {
        return acceptReplyStreamId;
    }

    public long acceptRef()
    {
        return 0L;
    }

    public long acceptCorrelationId()
    {
        return acceptCorrelationId;
    }

    public void setThrottle(MessageConsumer throttle)
    {
        this.router.setThrottle(acceptName, acceptReplyStreamId, throttle);
    }

    public abstract void abort();

    public abstract void complete();

}