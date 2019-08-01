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
package org.reaktivity.nukleus.http_cache.internal.streams.proxy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_cache.internal.test.HttpCacheCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

public class Rfc7240ProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/rfc7240");

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
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout).around(counters);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/acknowledge.prefer.wait.header.in.response/accept/client",
        "${streams}/acknowledge.prefer.wait.header.in.response/connect/server",
    })
    public void shouldAcknowledgePreferWaitHeaderInResponse() throws Exception
    {
        k3po.start();
        sleep(4000);
        k3po.notifyBarrier("PREFER_WAIT_REQUEST_ONE_COMPLETED");
        sleep(4000);
        k3po.notifyBarrier("PREFER_WAIT_REQUEST_TWO_COMPLETED");
        sleep(4000);
        k3po.notifyBarrier("PREFER_WAIT_REQUEST_THREE_COMPLETED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/missing.preference.applied.header.on.prefer.wait/accept/client",
        "${streams}/missing.preference.applied.header.on.prefer.wait/connect/server",
    })
    public void shouldHandleMissingPreferenceAppliedHeaderOnPreferWait() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_REQUEST_WAIT");
        sleep(4000);
        k3po.notifyBarrier("FIRST_REQUEST_RECEIVED");
        k3po.awaitBarrier("SECOND_REQUEST_WAIT");
        sleep(4000);
        k3po.notifyBarrier("SECOND_REQUEST_RECEIVED");
        k3po.awaitBarrier("THIRD_REQUEST_WAIT");
        sleep(4000);
        k3po.notifyBarrier("THIRD_REQUEST_RECEIVED");
        k3po.finish();
    }
}