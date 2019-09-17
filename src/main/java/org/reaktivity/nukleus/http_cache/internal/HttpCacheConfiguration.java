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
package org.reaktivity.nukleus.http_cache.internal;

import java.util.Random;

import org.reaktivity.nukleus.Configuration;

public class HttpCacheConfiguration extends Configuration
{
    public static final boolean DEBUG = Boolean.getBoolean("nukleus.http_cache.debug");

    public static final IntPropertyDef HTTP_CACHE_CAPACITY;
    public static final IntPropertyDef HTTP_CACHE_SLOT_CAPACITY;
    public static final IntPropertyDef HTTP_CACHE_MAXIMUM_REQUESTS;
    public static final IntPropertyDef HTTP_CACHE_ETAG_PREFIX;
    public static final IntPropertyDef HTTP_CACHE_ALLOWED_CACHE_PERCENTAGE;
    public static final IntPropertyDef HTTP_CACHE_PREFER_WAIT_MAXIMUM;

    private static final ConfigurationDef HTTP_CACHE_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.http_cache");
        HTTP_CACHE_CAPACITY = config.property("capacity", 1024 * 64 * 64);
        HTTP_CACHE_SLOT_CAPACITY = config.property("slot.capacity", 0x4000); // ALSO is max header size
        HTTP_CACHE_MAXIMUM_REQUESTS = config.property("maximum.requests", 64 * 1024);
        HTTP_CACHE_ETAG_PREFIX = config.property("etag.prefix", new Random().nextInt(99999));
        HTTP_CACHE_ALLOWED_CACHE_PERCENTAGE = config.property("allowed.cache.percentage", 95);
        HTTP_CACHE_PREFER_WAIT_MAXIMUM = config.property("prefer.wait.maximum", Integer.MAX_VALUE);
        HTTP_CACHE_CONFIG = config;
    }

    public HttpCacheConfiguration(
        Configuration config)
    {
        super(HTTP_CACHE_CONFIG, config);
    }

    public int cacheCapacity()
    {
        return HTTP_CACHE_CAPACITY.getAsInt(this);
    }

    public int cacheSlotCapacity()
    {
        return HTTP_CACHE_SLOT_CAPACITY.getAsInt(this);
    }

    public int allowedCachePercentage()
    {
        return HTTP_CACHE_ALLOWED_CACHE_PERCENTAGE.getAsInt(this);
    }

    public int maximumRequests()
    {
        return HTTP_CACHE_MAXIMUM_REQUESTS.getAsInt(this);
    }

    public int preferWaitMaximum()
    {
        return HTTP_CACHE_PREFER_WAIT_MAXIMUM.getAsInt(this);
    }
}
