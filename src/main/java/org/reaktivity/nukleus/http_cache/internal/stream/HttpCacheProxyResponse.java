package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;

public abstract class HttpCacheProxyResponse
{
    abstract void onThrottleBeforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    abstract void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);
}
