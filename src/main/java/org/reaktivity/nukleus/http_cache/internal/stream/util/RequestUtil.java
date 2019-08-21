package org.reaktivity.nukleus.http_cache.internal.stream.util;

public final class RequestUtil
{
    public static short authorizationScope(
        long authorization)
    {
        return (short) (authorization >>> 48);
    }

    public static int requestHash(
        short authorizationScope,
        int requestUrlHash)
    {
        return 31 * authorizationScope + requestUrlHash;
    }
}
