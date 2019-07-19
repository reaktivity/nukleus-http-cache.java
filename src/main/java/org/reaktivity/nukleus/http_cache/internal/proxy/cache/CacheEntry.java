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

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.PreferWaitIfNoneMatchRequest;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

import java.util.function.Consumer;

public abstract class CacheEntry
{
    final CacheableRequest cachedRequest;

    public CacheEntry(CacheableRequest request)
    {
        this.cachedRequest = request;
    }

    public abstract int requestUrl();

    public abstract void purge();

    public abstract boolean canServeRequest(
        ListFW<HttpHeaderFW> request,
        short authScope);

    public abstract void commit();

    abstract void recentAuthorizationHeader(String authorizationHeader);

    public abstract void serveClient(AnswerableByCacheRequest streamCorrelation);

    public abstract boolean expectSubscribers();

    public abstract boolean isUpdatedBy(CacheableRequest request);

    public abstract boolean doesNotVaryBy(CacheEntry cacheEntry);

    public abstract void subscribers(Consumer<PreferWaitIfNoneMatchRequest> consumer);

    public abstract boolean isSelectedForUpdate(CacheableRequest request);

    public abstract boolean isUpdateRequestForThisEntry(ListFW<HttpHeaderFW> requestHeaders);

    public abstract boolean subscribeWhenNoneMatch(PreferWaitIfNoneMatchRequest preferWaitRequest);

    abstract boolean canServeUpdateRequest(ListFW<HttpHeaderFW> request);

    protected abstract ListFW<HttpHeaderFW> getCachedResponseHeaders();

    protected abstract ListFW<HttpHeaderFW> getCachedResponseHeaders(ListFW<HttpHeaderFW> responseHeadersRO, BufferPool bp);

    protected abstract boolean isIntendedForSingleUser();

    public abstract void sendHttpPushPromise(AnswerableByCacheRequest request);
}
