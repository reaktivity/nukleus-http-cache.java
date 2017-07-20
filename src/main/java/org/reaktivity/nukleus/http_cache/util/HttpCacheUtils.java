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

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpCacheUtils
{

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
                case "cache-control":
                    if (value.contains("no-cache"))
                    {
                        return false;
                    }
                    return true;
                case ":method":
                    if ("GET".equalsIgnoreCase(value))
                    {
                        return false;
                    }
                    return true;
                default:
                    return false;
                }
        });
    }

    public static boolean hasStoredResponseThatSatisfies(
        String requestURL,
        int requestURLHash,
        ListFW<HttpHeaderFW> requestHeaders)
    {
        // NOTE: we never store anything right now so this always returns
        // false
        return false;
    }
}
