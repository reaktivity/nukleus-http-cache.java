/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.HTTP_CACHE_MAXIMUM_REQUESTS;

import org.junit.Test;

public class HttpCacheConfigurationTest
{
    // needed by test annotations
    public static final String HTTP_CACHE_MAXIMUM_REQUESTS_NAME = "nukleus.http_cache.maximum.requests";

    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(HTTP_CACHE_MAXIMUM_REQUESTS.name(), HTTP_CACHE_MAXIMUM_REQUESTS_NAME);
    }
}
