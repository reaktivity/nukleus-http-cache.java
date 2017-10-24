package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.SURROGATE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class SurrogateControl
{
    public static final String MAX_AGE = "max-age";
    private static final Pattern FRESHNESS_EXTENSION = Pattern.compile("\\s+\\d+=(?<ext>\\d+)\\s+");

    public static int getMaxAgeFreshnessExtension(
            ListFW<HttpHeaderFW> responseHeadersRO,
            CacheControl cacheControlParserFW)
    {
        String surrogateControl = getHeader(responseHeadersRO, SURROGATE_CONTROL);
        if (surrogateControl != null)
        {
            CacheControl parsedSurrogateControl = cacheControlParserFW.parse(surrogateControl);
            String maxage = parsedSurrogateControl.getValue(MAX_AGE);
            if (maxage != null)
            {
                Matcher matcher = FRESHNESS_EXTENSION.matcher(maxage);
                if(matcher.matches())
                {
                    return Integer.parseInt(matcher.group("ext"));
                }
            }
        }
        return -1;
    }
}
