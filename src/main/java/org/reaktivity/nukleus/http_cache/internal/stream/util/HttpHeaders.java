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

public final class HttpHeaders
{
    public static final String X_HTTP_CACHE_SYNC = "x-http-cache-sync";
    public static final String X_POLL_INJECTED = "x-poll-injected";
    public static final String X_RETRY_AFTER = "x-retry-after";
    public static final String STATUS = ":status";
    public static final String CACHE_CONTROL = "cache-control";
    public static final String IF_MATCH = "if-match";
    public static final String IF_NONE_MATCH = "if-none-match";
    public static final String IF_MODIFIED_SINCE = "if-modified-since";
    public static final String SCHEME = ":scheme";
    public static final String AUTHORITY = ":authority";
    public static final String PATH = ":path";
    public static final String CONTENT_LENGTH = "content-length";
    public static final String TRANSFER_ENCODING = "transfer-encoding";
    public static final String METHOD = ":method";

    // values
    public static final String X_HTTP_CACHE_SYNC_ALWAYS = "always";

    private HttpHeaders()
    {
        // utility class
    }

}
