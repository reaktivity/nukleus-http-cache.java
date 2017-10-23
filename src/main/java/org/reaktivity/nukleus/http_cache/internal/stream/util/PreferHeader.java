package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class PreferHeader
{

    public static final String WAIT = "wait";

    public static String getWait(
            ListFW<HttpHeaderFW> requestHeaders,
            CacheControl cacheControlParserFW)
    {
        String surrogateControl = getHeader(requestHeaders, PREFER);
        if (surrogateControl != null)
        {
            CacheControl parsedSurrogateControl = cacheControlParserFW.parse(surrogateControl);
            return parsedSurrogateControl.getValue(WAIT);
        }
        return null;
    }

    public static final String ONLY_WHEN_MODIFIED = "only-when-modified";

    public static final Predicate<? super HttpHeaderFW> PREFER_RESPONSE_WHEN_MODIFIED = h ->
    {
        final String name = h.name().asString();
        final String value = h.name().asString();
        return PREFER.equals(name) && value.contains(ONLY_WHEN_MODIFIED);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_HEADER = h ->
    {
        final String name = h.name().asString();
        return PREFER.equals(name);
    };
}
