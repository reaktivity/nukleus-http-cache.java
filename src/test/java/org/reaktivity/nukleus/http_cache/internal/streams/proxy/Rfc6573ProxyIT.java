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
package org.reaktivity.nukleus.http_cache.internal.streams.proxy;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_cache.internal.test.HttpCacheCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class Rfc6573ProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/rfc6573");

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
        "${streams}/invalidate.cache.when.collection.relative.path/accept/client",
        "${streams}/invalidate.cache.when.collection.relative.path/connect/server",
    })
    public void shouldInvalidateCacheWhenCollectionRelativePath() throws Exception
    {
        k3po.finish();
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/invalidate.cache.when.collection.same.origin/accept/client",
        "${streams}/invalidate.cache.when.collection.same.origin/connect/server",
    })
    public void shouldInvalidateCacheWhenCollectionSameOrigin() throws Exception
    {
        k3po.finish();
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/invalidate.cache.with.multiple.link.target/accept/client",
        "${streams}/invalidate.cache.with.multiple.link.target/connect/server",
    })
    public void shouldInvalidateCachWithMultipleLinkTargets() throws Exception
    {
        k3po.finish();
        counters.assertRequestsSlotsAndRequestGroups(0);
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/ignore.link.when.collection.cross.origin/accept/client",
        "${streams}/ignore.link.when.collection.cross.origin/connect/server",
    })
    public void shouldIgnoreLinkWhenCollectionCrossOrigin() throws Exception
    {
        k3po.finish();
        counters.assertRequestsSlotsAndRequestGroups(0);
    }
}
