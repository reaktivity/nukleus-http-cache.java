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

public final class RequestUtil
{
    public static short authorizationScope(
        long authorization)
    {
        return (short) (authorization >>> 48);
    }

    public static int requestHash(
        short authorizationScope,
        int requestURLHash)
    {
        return 31 * authorizationScope + requestURLHash;
    }

    private RequestUtil()
    {
        // utility
    }
}
