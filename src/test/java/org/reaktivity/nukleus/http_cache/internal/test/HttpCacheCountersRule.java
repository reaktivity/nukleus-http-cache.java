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
package org.reaktivity.nukleus.http_cache.internal.test;

import static org.junit.Assert.assertEquals;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.reaktor.test.ReaktorRule;

public class HttpCacheCountersRule implements TestRule
{
    private static final int NUM_OF_SLOTS_PER_CACHE_ENTRY = 3;

    private final ReaktorRule reaktor;

    public HttpCacheCountersRule(
        ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(
        Statement base,
        Description description)
    {
        return base;
    }

    public long requests()
    {
        return reaktor.counter("http-cache.requests");
    }

    public long requestsCachable()
    {
        return reaktor.counter("http-cache.requests.cacheable");
    }

    public long requestsPreferWait()
    {
        return reaktor.counter("http-cache.requests.prefer.wait");
    }

    public long responses()
    {
        return reaktor.counter("http-cache.responses");
    }

    public long responsesCached()
    {
        return reaktor.counter("http-cache.responses.cached");
    }

    public long groupRequestsCacheable()
    {
        return reaktor.counter("http-cache.group.requests.cacheable");
    }

    public long responsesAborted()
    {
        return reaktor.counter("http-cache.responses.aborted");
    }

    public long promises()
    {
        return reaktor.counter("http-cache.promises");
    }

    public long promisesCanceled()
    {
        return reaktor.counter("http-cache.promises.canceled");
    }

    public long cacheEntries()
    {
        return reaktor.counter("http-cache.cache.entries");
    }

    public long refreshRequests()
    {
        return reaktor.counter("http-cache.refresh.request.acquires");
    }

    public long cachedRequestAcquires()
    {
        return reaktor.counter("http-cache.cached.request.acquires");
    }

    public long cachedRequestReleases()
    {
        return reaktor.counter("http-cache.cached.request.releases");
    }

    public long cachedResponseAcquires()
    {
        return reaktor.counter("http-cache.cached.response.acquires");
    }

    public long cachedResponseReleases()
    {
        return reaktor.counter("http-cache.cached.response.releases");
    }

    private long requestSlots()
    {
        return reaktor.counter("http-cache.request.acquires") -
               reaktor.counter("http-cache.request.releases");
    }

    public long cacheSlots()
    {
        return cachedRequestAcquires() + cachedResponseAcquires() - cachedRequestReleases() - cachedResponseReleases();
    }

    public void assertExpectedCacheEntries(
        int numberOfResponses)
    {
        assertEquals(numberOfResponses, cacheEntries());
        assertEquals(NUM_OF_SLOTS_PER_CACHE_ENTRY * numberOfResponses, cacheSlots());
    }

    public void assertExpectedCacheRefreshes(
        int cacheInitiatedRefreshes)
    {
        assertEquals(cacheInitiatedRefreshes, refreshRequests());
    }

    public void assertExpectedCacheEntries(
        int numberOfResponses,
        int cacheInitiatedRefreshes)
    {
        assertExpectedCacheEntries(numberOfResponses);
        assertExpectedCacheRefreshes(cacheInitiatedRefreshes);
    }

    public void assertRequestsSlots(
        int expected)
    {
        assertEquals(expected, requestSlots());
    }

    public void assertRequests(
        int expected)
    {
        assertEquals(expected, requests());
    }

    public void assertRequestsCacheable(
        int expected)
    {
        assertEquals(expected, requestsCachable());
    }

    public void assertRequestsPreferWait(
        int expected)
    {
        assertEquals(expected, requestsPreferWait());
    }

    public void assertResponses(
        int expected)
    {
        assertEquals(expected, responses());
    }

    public void assertResponsesCached(
        int expected)
    {
        assertEquals(expected, Math.max(responsesCached() - groupRequestsCacheable(), 0));
    }

    public void assertResponsesAborted(
        int expected)
    {
        assertEquals(expected, responsesAborted());
    }

    public void assertPromises(
        int expected)
    {
        assertEquals(expected, promises());
    }
}
