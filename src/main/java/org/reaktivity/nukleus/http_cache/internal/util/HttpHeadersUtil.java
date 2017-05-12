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

package org.reaktivity.nukleus.http_cache.internal.util;

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

// TODO move into ListFW and HttpHeaderFW by implementing stream
public final class HttpHeadersUtil
{

    public static final String INJECTED_HEADER_NAME = "x-poll-injected";
    public static final String INJECTED_HEADER_DEFAULT_VALUE = "http-cache-sync";
    public static final String INJECTED_HEADER_AND_NO_CACHE_VALUE = INJECTED_HEADER_DEFAULT_VALUE + ", no-cache";
    public static final String POLL_HEADER_NAME = "x-poll-interval";

    public static final Predicate<HttpHeaderFW> PUSH_TIMER_FILTER =
            h -> INJECTED_HEADER_NAME.equals(h.name().toString());

    public static final Predicate<HttpHeaderFW> IS_POLL_HEADER =
                    h -> POLL_HEADER_NAME.equals(h.name().asString());

    public static void forAllMatch(ListFW<HttpHeaderFW> headers, Predicate<HttpHeaderFW> predicate,
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
}
