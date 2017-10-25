/**
 * Copyright 2016-2017 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class EdgeArchProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_cache/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_cache/streams/proxy/edge-arch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024)
            .nukleus("http-cache"::equals)
            .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.inject.on.post/accept/client",
        "${streams}/does.not.inject.on.post/connect/server",
        })
    public void shouldNotInjectOnPost() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.inject.on.uncacheable.response/accept/client",
        "${streams}/does.not.inject.on.uncacheable.response/connect/server",
    })
    public void shouldNotInjectOnUncacheableResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/serve.from.cache.when.freshness.extension.is.valid.and.x-protected/accept/client",
        "${streams}/serve.from.cache.when.freshness.extension.is.valid.and.x-protected/connect/server",
    })
    public void serveFromCacheWhenFreshnessExtensionIsValidAndXProtected() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.share.debounce.when.explicitly.private.cache/accept/client",
        "${streams}/does.not.share.debounce.when.explicitly.private.cache/connect/server",
    })
    public void doesNotShareDebounceWhenExplicitlyPrivate() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.share.debounce.when.implied.private.cache/accept/client",
        "${streams}/does.not.share.debounce.when.implied.private.cache/connect/server",
    })
    public void shouldNotShareDebounceWhenImpliedPrivate() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.share.debounce.when.varies/accept/client",
        "${streams}/does.not.share.debounce.when.varies/connect/server",
    })

    public void shouldNotShareDebounceWhenVaries() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/does.not.share.with.different.protected.scope/accept/client",
        "${streams}/does.not.share.with.different.protected.scope/connect/server",
    })

    public void doesNotShareWithDifferentProtectedScope() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/freshness-extension.inject.individualized.push.promises/accept/client",
        "${streams}/freshness-extension.inject.individualized.push.promises/connect/server",
    })
    public void shouldInjectIndividualizedPushPromisesOnSharedFreshnessExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/inject.stale-while-revalidate.push-promise.no-cache/accept/client",
        "${streams}/inject.stale-while-revalidate.push-promise.no-cache/connect/server",
    })
    public void shouldInjectValuesOnFreshnessExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/share.debounce.when.explicitly.public/accept/client",
        "${streams}/share.debounce.when.explicitly.public/connect/server",
    })

    public void shouldShareDebounceWhenExplicitlyPublic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore
    @Specification({
        "${route}/proxy/controller",
        "${streams}/share.debounce.when.x-protected.and.same.scope/accept/client",
        "${streams}/share.debounce.when.x-protected.and.same.scope/connect/server",
    })
    public void shouldShareDebounceWhenXProtectedAndSameScope() throws Exception
    {
        k3po.finish();
    }

}
