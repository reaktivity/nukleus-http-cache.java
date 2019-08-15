package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;

abstract class HttpCacheProxyResponse
{
    abstract void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    abstract void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);
}
