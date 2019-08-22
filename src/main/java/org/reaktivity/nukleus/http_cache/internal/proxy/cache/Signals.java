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

public final class Signals
{
    public static final long CACHE_ENTRY_UPDATED_SIGNAL = 1L;
    public static final long CACHE_ENTRY_SIGNAL = 2L;
    public static final long ABORT_SIGNAL = 3L;
    public static final long REQUEST_EXPIRED_SIGNAL = 4L;
    public static final long INITIATE_REQUEST_SIGNAL = 5L;
}
