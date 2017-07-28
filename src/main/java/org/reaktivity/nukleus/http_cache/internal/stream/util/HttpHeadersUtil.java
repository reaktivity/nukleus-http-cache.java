/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.NO_CACHE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.CACHEABLE_BY_DEFAULT_STATUS_CODES;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.X_HTTP_CACHE_SYNC;

import java.util.Iterator;
import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpHeadersUtil
{
    public static final String INJECTED_HEADER_AND_NO_CACHE_VALUE = X_HTTP_CACHE_SYNC + ", no-cache";

    public static final Predicate<HttpHeaderFW> SHOULD_POLL =
                    h -> HttpHeaders.X_RETRY_AFTER.equals(h.name().asString());


    public static final Predicate<? super HttpHeaderFW> INJECTED_DEFAULT_HEADER = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        return HttpHeaders.X_POLL_INJECTED.equals(name) && X_HTTP_CACHE_SYNC.equals(value);
    };

    public static final Predicate<? super HttpHeaderFW> INJECTED_HEADER_AND_NO_CACHE = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        return HttpHeaders.X_POLL_INJECTED.equals(name) && INJECTED_HEADER_AND_NO_CACHE_VALUE.equals(value);
    };

    public static final Predicate<? super HttpHeaderFW> NO_CACHE_CACHE_CONTROL = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        // TODO proper parsing of value
        return "cache-control".equals(name) && value.contains("no-cache");
    };

    public static String getRequestURL(ListFW<HttpHeaderFW> headers)
    {
        // TODO, less garbage collection...
        // streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder scheme = new StringBuilder();
        final StringBuilder path = new StringBuilder();
        final StringBuilder authority = new StringBuilder();
        headers.forEach(h ->
        {
            switch (h.name().asString())
            {
                case HttpHeaders.AUTHORITY:
                    authority.append(h.value().asString());
                    break;
                case HttpHeaders.PATH:
                    path.append(h.value().asString());
                    break;
                case HttpHeaders.SCHEME:
                    scheme.append(h.value().asString());
                    break;
                default:
                    break;
                }
        });
        return scheme.append(authority.toString()).append(path.toString()).toString();
    }

    public static String getHeader(ListFW<HttpHeaderFW> cachedRequestHeadersRO, String headerName)
    {
        // TODO remove GC when have streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder header = new StringBuilder();
        cachedRequestHeadersRO.forEach(h ->
        {
            if (headerName.equals(h.name().asString()))
            {
                header.append(h.value().asString());
            }
        });

        return header.length() == 0 ? null : header.toString();
    }

    public static boolean isCacheableResponse(ListFW<HttpHeaderFW> responseHeaders)
    {
        String cacheControl = getHeader(responseHeaders, "cache-control");
        if (cacheControl != null)
        {
            HttpCacheUtils.CacheControlParser parser = new  HttpCacheUtils.CacheControlParser(cacheControl);
            Iterator<String> iter = parser.iterator();
            while(iter.hasNext())
            {
                String directive = iter.next();
                switch(directive)
                {
                    // TODO expires
                    case NO_CACHE:
                        return false;
                    case CacheDirectives.PRIVATE:
                        return false;
                    case CacheDirectives.PUBLIC:
                        return true;
                    case CacheDirectives.MAX_AGE:
                        return true;
                    case CacheDirectives.S_MAXAGE:
                        return true;
                    default:
                        break;
                }
            }
        }
        return responseHeaders.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            if (STATUS.equals(name))
            {
                return CACHEABLE_BY_DEFAULT_STATUS_CODES.contains(value);
            }
            return false;
        });
    }

    private HttpHeadersUtil()
    {
        // utility class
    }
}
