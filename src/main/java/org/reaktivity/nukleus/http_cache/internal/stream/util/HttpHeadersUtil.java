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

import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpHeadersUtil
{
    public static final Predicate<? super HttpHeaderFW> HAS_CACHE_CONTROL = h ->
    {
        String name = h.name().asString();
        return "cache-control".equals(name);
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

    public static String getHeaderOrDefault(
            ListFW<HttpHeaderFW> responseHeaders,
            String headerName,
            String defaulted)
    {
        final String result = getHeader(responseHeaders, headerName);
        return result == null ? defaulted : result;
    }
}
