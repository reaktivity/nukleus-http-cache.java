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
package org.reaktivity.nukleus.http_cache.internal;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class HttpCacheCounters
{
    public final Function<String, LongSupplier> supplyCounter;
    public final Function<String, LongConsumer> supplyAccumulator;

    public final LongSupplier requests;
    public final LongSupplier requestsCacheable;
    public final LongSupplier groupRequestsCacheable;
    public final LongSupplier requestsRetry;
    public final LongSupplier responses;
    public final LongSupplier responsesRetry;
    public final LongSupplier responsesNotModified;
    public final LongSupplier groupResponsesCacheable;
    public final LongSupplier responsesCached;
    public final LongSupplier responsesPurged;
    public final LongSupplier responsesAbortedVary;
    public final LongSupplier promises;
    public final LongSupplier cachePurgeAttempts;
    public final LongConsumer cacheEntries;
    public final LongConsumer frequencyBuckets;
    public final LongConsumer requestGroups;

    public HttpCacheCounters(
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.supplyCounter = supplyCounter;
        this.supplyAccumulator = supplyAccumulator;

        this.requests = supplyCounter.apply("http-cache.requests");
        this.requestsCacheable = supplyCounter.apply("http-cache.requests.cacheable");
        this.groupRequestsCacheable = supplyCounter.apply("http-cache.group.requests.cacheable");
        this.requestsRetry = supplyCounter.apply("http-cache.requests.retry");
        this.responses = supplyCounter.apply("http-cache.responses");
        this.groupResponsesCacheable = supplyCounter.apply("http-cache.group.responses.cacheable");
        this.responsesRetry = supplyCounter.apply("http-cache.responses.retry");
        this.responsesNotModified = supplyCounter.apply("http-cache.responses.not.modified");
        this.responsesCached = supplyCounter.apply("http-cache.responses.cached");
        this.responsesAbortedVary = supplyCounter.apply("http-cache.responses.aborted.vary");
        this.responsesPurged = supplyCounter.apply("http-cache.responses.purge");
        this.promises = supplyCounter.apply("http-cache.promises");
        this.cacheEntries = supplyAccumulator.apply("http-cache.cache.entries");
        this.requestGroups = supplyAccumulator.apply("http-cache.request.groups");
        this.frequencyBuckets = supplyAccumulator.apply("http-cache.frequency.buckets");
        this.cachePurgeAttempts = supplyCounter.apply("http-cache.cache.purge.attempts");
    }
}
