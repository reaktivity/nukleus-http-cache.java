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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.NO_CACHE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.NO_STORE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.PUBLIC;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.S_MAXAGE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.METHOD;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.TRANSFER_ENCODING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.Iterator;
import java.util.List;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpCacheUtils
{

    public static final List<String> CACHEABLE_BY_DEFAULT_STATUS_CODES = unmodifiableList(
            asList("200", "203", "204", "206", "300", "301", "404", "405", "410", "414", "501"));

    public static final String LAST_MODIFIED = "last-modified";

    private HttpCacheUtils()
    {
        // utility class
    }

    public static boolean canBeServedByCache(
        ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
                case CACHE_CONTROL:
                    return value.contains("no-cache");
                case METHOD:
                    return !"GET".equalsIgnoreCase(value);
                case CONTENT_LENGTH:
                    return true;
                case TRANSFER_ENCODING:
                    return true;
                default:
                    return false;
                }
        });
    }

    public static boolean canInjectPushPromise(
            ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
            case METHOD:
                return !"GET".equalsIgnoreCase(value);
            case CONTENT_LENGTH:
                return true;
            default:
                return false;
            }
        });
    }

    public static boolean isCacheControlNoStore(HttpHeaderFW header)
    {
        final String name = header.name().asString();
        final String value = header.value().asString();
        return HttpHeaders.CACHE_CONTROL.equals(name) && value.contains(NO_STORE);
    }

    public static boolean isCacheable(ListFW<HttpHeaderFW> response)
    {
        if (response.anyMatch(h ->
                CACHE_CONTROL.equals(h.name().asString())
                && h.value().asString().contains("private")))
        {
            return false;
        }
        return isPrivatelyCacheable(response);
    }

    public static boolean isPrivatelyCacheable(ListFW<HttpHeaderFW> response)
    {
        String cacheControl = getHeader(response, "cache-control");
        if (cacheControl != null)
        {
            CacheControl parser = new CacheControl().parse(cacheControl);
            Iterator<String> iter = parser.iterator();
            while(iter.hasNext())
            {
                String directive = iter.next();
                switch(directive)
                {
                    // TODO expires
                    case NO_CACHE:
                        return false;
                    case PUBLIC:
                        return true;
                    case MAX_AGE:
                        return true;
                    case S_MAXAGE:
                        return true;
                    default:
                        break;
                }
            }
        }
        return response.anyMatch(h ->
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
}
