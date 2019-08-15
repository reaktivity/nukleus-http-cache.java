package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

abstract class HttpCacheProxyRequest
{
    private long acceptReplyId;

    HttpCacheProxyRequest(
        long acceptReplyId)
    {

        this.acceptReplyId = acceptReplyId;
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

    abstract HttpCacheProxyResponse newResponse(
        ListFW<HttpHeaderFW> responseHeaders);

    public long acceptReplyId()
    {
        return acceptReplyId;
    }
}
