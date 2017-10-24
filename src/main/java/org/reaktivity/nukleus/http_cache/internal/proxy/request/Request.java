package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import org.reaktivity.nukleus.function.MessageConsumer;

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

    public Request(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId)
    {
        this.acceptName = acceptName;
        this.acceptReply = acceptReply;
        this.acceptReplyStreamId = acceptReplyStreamId;
        this.acceptCorrelationId = acceptCorrelationId;
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

    public abstract void abort();

    public abstract void complete();

}