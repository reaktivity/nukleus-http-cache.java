/**
 * Copyright 2016-2019 The Reaktivity Project
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

import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE_0;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class PreferHeader
{
    public static boolean isPreferIfNoneMatch(
        ListFW<HttpHeaderFW> headers)
    {
        return getHeader(headers, IF_NONE_MATCH) != null &&
               headers.anyMatch(PREFER_HEADER_NAME);
    }

    public static boolean isPreferMaxAgeZero(
        ListFW<HttpHeaderFW> headers)
    {
        String cacheControl = getHeader(headers, CACHE_CONTROL);
        return headers.anyMatch(PREFER_HEADER_NAME) &&
               cacheControl != null && cacheControl.contains(MAX_AGE_0);
    }

    public static boolean isPreferWait(
        ListFW<HttpHeaderFW> headers)
    {
        String prefer = getHeader(headers, PREFER);
        return prefer != null && prefer.toLowerCase().startsWith("wait=");
    }

    public static boolean isPreferenceApplied(
        ListFW<HttpHeaderFW> headers)
    {
        String preferecenceApplied = getHeader(headers, PREFERENCE_APPLIED);
        return preferecenceApplied != null;
    }

    public static final Predicate<? super HttpHeaderFW> PREFER_HEADER_NAME = h ->
    {
        final String name = h.name().asString();
        return PREFER.equals(name);
    };

    public static int getPreferWait(ListFW<HttpHeaderFW> headers)
    {
        String wait = getHeader(headers, PREFER);
        if (wait != null)
        {
            return wait.toLowerCase().startsWith("wait=") ? Integer.parseInt(wait.split("=")[1]) : 0;
        }
        return 0;
    }

    private PreferHeader()
    {
        // utility
    }
}
