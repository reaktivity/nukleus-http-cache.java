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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

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
