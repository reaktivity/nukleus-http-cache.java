/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORITY;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.EMULATED_PROTOCOL_STACK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.RETRY_AFTER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;

public final class HttpHeadersUtil
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    public static final Predicate<? super HttpHeaderFW> HAS_CACHE_CONTROL = h ->
    {

        String name = h.name().asString();
        return "cache-control".equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_IF_NONE_MATCH = h ->
    {
        String name = h.name().asString();
        return IF_NONE_MATCH.equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_AUTHORIZATION = h ->
    {
        String name = h.name().asString();
        return AUTHORITY.equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_EMULATED_PROTOCOL_STACK = h ->
    {
        String name = h.name().asString();
        return EMULATED_PROTOCOL_STACK.equalsIgnoreCase(name);
    };

    public static final Predicate<? super HttpHeaderFW> HAS_RETRY_AFTER = h ->
    {
        String name = h.name().asString();
        return RETRY_AFTER.equalsIgnoreCase(name);
    };

    public static String getRequestURL(
        Array32FW<HttpHeaderFW> headers)
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
            case AUTHORITY:
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
        return scheme.append("://").append(authority.toString()).append(path.toString()).toString();
    }

    public static String getHeader(
        Array32FW<HttpHeaderFW> cachedRequestHeaders,
        String headerName)
    {
        // TODO remove GC when have streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder header = new StringBuilder();
        cachedRequestHeaders.forEach(h ->
        {
            if (headerName.equalsIgnoreCase(h.name().asString()))
            {
                header.append(h.value().asString());
            }
        });

        return header.length() == 0 ? null : header.toString();
    }

    public static String getHeaderOrDefault(
            Array32FW<HttpHeaderFW> responseHeaders,
            String headerName,
            String defaulted)
    {
        final String result = getHeader(responseHeaders, headerName);
        return result == null ? defaulted : result;
    }

    public static boolean hasStatusCode(
        Array32FW<HttpHeaderFW> responseHeaders,
        int statusCode)
    {
        return responseHeaders.anyMatch(h ->
                STATUS.equals(h.name().asString()) && (Integer.toString(statusCode)).equals(h.value().asString()));
    }

    public static boolean retry(
        Array32FW<HttpHeaderFW> responseHeaders)
    {
        return hasStatusCode(responseHeaders, 503) && responseHeaders.anyMatch(HAS_RETRY_AFTER);
    }

    /*
     * Retry-After supports two formats. For example:
     * Retry-After: Wed, 21 Oct 2015 07:28:00 GMT
     * Retry-After: 120
     *
     * @return wait time in seconds from now for both formats
     */
    public static long retryAfter(
        Array32FW<HttpHeaderFW> responseHeaders)
    {
        HttpHeaderFW header = responseHeaders.matchFirst(HAS_RETRY_AFTER);

        if (header == null)
        {
            return 0L;
        }

        String retryAfter = header.value().asString();
        try
        {
            if (retryAfter != null && !retryAfter.isEmpty())
            {
                if (Character.isDigit(retryAfter.charAt(0)))
                {
                    return Integer.valueOf(retryAfter);
                }
                else
                {
                    final Date date = DATE_FORMAT.parse(retryAfter);
                    final long epochSecond = date.toInstant().getEpochSecond();
                    final long nowSecond = Instant.now().getEpochSecond();
                    final long waitSeconds = epochSecond - nowSecond;
                    return Math.max(waitSeconds, 0);
                }
            }
        }
        catch (Exception e)
        {
            // ignore
        }
        return 0L;
    }

    private HttpHeadersUtil()
    {
        // utility
    }
}
