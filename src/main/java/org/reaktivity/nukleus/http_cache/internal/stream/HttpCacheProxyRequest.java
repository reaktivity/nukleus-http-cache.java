package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public abstract class HttpCacheProxyRequest
{


    HttpCacheProxyRequest(
        HttpCacheProxyFactory streamFactory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long acceptReplyId,
        MessageConsumer connectInitial,
        MessageConsumer connectReply,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {


    }

    abstract void onRequestMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    abstract void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    public abstract MessageConsumer newResponse(
        ListFW<HttpHeaderFW> responseHeaders);

}
