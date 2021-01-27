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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;

public final class PreferHeader
{
    public static boolean isPreferIfNoneMatch(
        Array32FW<HttpHeaderFW> headers)
    {
        return getHeader(headers, IF_NONE_MATCH) != null &&
               headers.anyMatch(PREFER_HEADER_NAME);
    }

    public static boolean isPreferWait(
        Array32FW<HttpHeaderFW> headers)
    {
        String prefer = getHeader(headers, PREFER);
        return prefer != null && prefer.toLowerCase().startsWith("wait=");
    }

    public static boolean isPreferenceApplied(
        Array32FW<HttpHeaderFW> headers)
    {
        String preferenceApplied = getHeader(headers, PREFERENCE_APPLIED);
        return preferenceApplied != null;
    }

    public static final Predicate<? super HttpHeaderFW> PREFER_HEADER_NAME = h ->
    {
        final String name = h.name().asString();
        return PREFER.equals(name);
    };

    public static int getPreferWait(Array32FW<HttpHeaderFW> headers)
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
