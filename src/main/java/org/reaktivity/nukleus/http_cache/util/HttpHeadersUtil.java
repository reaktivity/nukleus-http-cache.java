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
package org.reaktivity.nukleus.http_cache.util;

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class HttpHeadersUtil {

    public static final String CACHE_SYNC = "x-http-cache-sync";
    public static final String INJECTED_HEADER_NAME = "x-poll-injected";
    public static final String INJECTED_HEADER_DEFAULT_VALUE = CACHE_SYNC;
    public static final String INJECTED_HEADER_AND_NO_CACHE_VALUE = INJECTED_HEADER_DEFAULT_VALUE + ", no-cache";
    public static final String POLL_HEADER_NAME = "x-retry-after";

    public static final Predicate<? super HttpHeaderFW> IS_INJECTED_HEADER =
            h -> INJECTED_HEADER_NAME.equals(h.name().asString());

    public static final Predicate<HttpHeaderFW> PUSH_TIMER_FILTER =
            h -> INJECTED_HEADER_NAME.equals(h.name().toString());

    public static final Predicate<HttpHeaderFW> IS_POLL_HEADER =
                    h -> POLL_HEADER_NAME.equals(h.name().asString());

    public static void forEachMatch(ListFW<HttpHeaderFW> headers, Predicate<HttpHeaderFW> predicate,
            Consumer<HttpHeaderFW> consumer)
    {
        headers.forEach(header ->
        {
            if(predicate.test(header))
            {
                consumer.accept(header);
            }
        });
    }

    public static final Predicate<? super HttpHeaderFW> INJECTED_DEFAULT_HEADER = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        return INJECTED_HEADER_NAME.equals(name) && INJECTED_HEADER_DEFAULT_VALUE.equals(value);
    };

    public static final Predicate<? super HttpHeaderFW> INJECTED_HEADER_AND_NO_CACHE = h ->
    {
        String name = h.name().asString();
        String value = h.value().asString();
        return INJECTED_HEADER_NAME.equals(name) && INJECTED_HEADER_AND_NO_CACHE_VALUE.equals(value);
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
                case ":authority":
                    authority.append(h.value().asString());
                    break;
                case ":path":
                    path.append(h.value().asString());
                    break;
                case ":scheme":
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
            if(headerName.equals(h.name().asString()))
            {
                // TODO multiple list values?
                header.append(h.value().asString());
            }
        });

        return header.length() == 0 ? null : header.toString();
    }
}
