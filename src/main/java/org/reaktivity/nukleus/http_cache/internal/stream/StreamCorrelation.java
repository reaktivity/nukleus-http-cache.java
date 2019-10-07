package org.reaktivity.nukleus.http_cache.internal.stream;

import org.reaktivity.nukleus.function.MessageConsumer;

final class StreamCorrelation
{
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;
    private final MessageConsumer connectInitial;
    private final long connectRouteId;
    private final long connectInitialId;
    private final long connectReplyId;

    StreamCorrelation(
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptInitialId,
        long acceptReplyId,
        MessageConsumer connectInitial,
        long connectRouteId,
        long connectInitialId,
        long connectReplyId)
    {
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectInitial = connectInitial;
        this.connectRouteId = connectRouteId;
        this.connectInitialId = connectInitialId;
        this.connectReplyId = connectReplyId;
    }

    public MessageConsumer getAcceptReply()
    {
        return acceptReply;
    }

    public long getAcceptRouteId()
    {
        return acceptRouteId;
    }
}
