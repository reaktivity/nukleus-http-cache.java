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
package org.reaktivity.nukleus.http_cache.internal.streams.proxy;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfigurationTest.HTTP_CACHE_ALLOWED_CACHE_EVICTION_COUNT_NAME;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfigurationTest.HTTP_CACHE_ALLOWED_CACHE_PERCENTAGE_NAME;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfigurationTest.HTTP_CACHE_CAPACITY_NAME;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfigurationTest.HTTP_CACHE_SLOT_CAPACITY_NAME;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_cache.internal.test.HttpCacheCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ProxyBehaviourIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/behavior");

    private final TestRule timeout = new DisableOnDebug(new Timeout(25, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .nukleus("http-cache"::equals)
            .controller("http-cache"::equals)
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(16384)
            .nukleus("http-cache"::equals)
            .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
            .clean();

    private final HttpCacheCountersRule counters = new HttpCacheCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(counters).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.sent.abort/accept/client",
        "${streams}/accept.sent.abort/connect/server",
    })
    public void shouldAcceptSentAbort() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Ignore("Request not processed until write closed")
    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.sent.abort.cacheable.request/accept/client",
        "${streams}/accept.sent.abort.cacheable.request/connect/server",
    })
    public void shouldHandleAbortSentOnCacheableRequest() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.requestsCachable());
        counters.assertRequestsSlotsAndRequestGroups(0);
        // We proceed with request out back anyways, TODO, consider adding to test response returning and getting cached
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
         "${streams}/connect.reply.sent.abort/accept/client",
         "${streams}/connect.reply.sent.abort/connect/server",
    })
    public void shouldConnectReplySentAbort() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Ignore("Request not processed until write closed")
    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/connect.sent.reset.on.cacheable.request/accept/client",
        "${streams}/connect.sent.reset.on.cacheable.request/connect/server",
    })
    public void shouldConnectSentResetOnCacheableRequest() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/connect.sent.reset/accept/client",
        "${streams}/connect.sent.reset/connect/server",
    })
    public void shouldConnectSentReset() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/accept.reply.sent.reset/accept/client",
        "${streams}/accept.reply.sent.reset/connect/server",
    })
    public void shouldAcceptReplySentReset() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Ignore("https://github.com/reaktivity/nukleus-http-cache.java/issues/72")
    @Specification({
        "${route}/proxy/controller",
        "${streams}/client.sent.abort.on.scheduled.poll/accept/client"
    })
    public void shouldAcceptAbortOnScheduledPoll() throws Exception
    {
        k3po.finish();
        //counters.assertExpectedCacheEntries(0); // TODO, fix. Sporadically failing today,
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/reset.connect.reply.if.accept.reply.reset/accept/client",
        "${streams}/reset.connect.reply.if.accept.reply.reset/connect/server",
    })
    public void shouldResetConnectReplyIfAcceptReplyReset() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Configure(name = HTTP_CACHE_CAPACITY_NAME, value = "32768")  //8 buffer slots
    @Configure(name = HTTP_CACHE_SLOT_CAPACITY_NAME, value = "4096")
    @Configure(name = HTTP_CACHE_ALLOWED_CACHE_PERCENTAGE_NAME, value = "75")
    @Configure(name = HTTP_CACHE_ALLOWED_CACHE_EVICTION_COUNT_NAME, value = "1")
    @Specification({
        "${route}/proxy/controller",
        "${streams}/purge.cache.entry.on.full.cache/accept/client",
        "${streams}/purge.cache.entry.on.full.cache/connect/server",
    })
    public void shouldPurgeCacheEntryOnFullCache() throws Exception
    {
        k3po.finish();
        counters.assertExpectedCacheEntries(1);
        counters.assertRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/reset.stream.if.group.request.already.dequeued/accept/client",
        "${streams}/reset.stream.if.group.request.already.dequeued/connect/server",
    })
    public void shouldResetStreamIfGroupRequestAlreadyDequeued() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RESPONSE_ONE_RECEIVED");
        k3po.notifyBarrier("SEND_GROUP_RESPONSE_ONE");
        k3po.finish();
        counters.assertExpectedCacheEntries(0);
        counters.assertRequestsSlotsAndRequestGroups(1);
    }
}
