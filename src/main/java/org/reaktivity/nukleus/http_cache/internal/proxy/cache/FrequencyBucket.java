/**
 * Copyright 2016-2020 The Reaktivity Project
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

import org.agrona.collections.ObjectHashSet;

public class FrequencyBucket
{
    private final ObjectHashSet<DefaultCacheEntry> entries;
    private final int frequency;

    public FrequencyBucket(
        int frequency)
    {
        this.frequency = frequency;
        this.entries = new ObjectHashSet<>();
    }

    public int frequency()
    {
        return frequency;
    }

    public ObjectHashSet<DefaultCacheEntry> entries()
    {
        return entries;
    }
}
