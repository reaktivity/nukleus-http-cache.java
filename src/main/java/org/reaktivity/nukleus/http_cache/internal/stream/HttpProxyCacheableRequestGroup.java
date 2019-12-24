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
package org.reaktivity.nukleus.http_cache.internal.stream;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;

import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;

public final class HttpProxyCacheableRequestGroup
{
    private final Map<String, Deque<HttpCacheProxyCacheableRequest>> queuedRequestsByEtag;
    private final Set<HttpCacheProxyCachedResponse> flushableResponses;
    private final HttpCacheProxyFactory factory;
    private final IntConsumer cleaner;
    private final int requestHash;

    private String authorizationHeader;
    private HttpCacheProxyGroupRequest groupRequest;
    private String ifNoneMatch;
    private DefaultCacheEntry cacheEntry;

    HttpProxyCacheableRequestGroup(
        HttpCacheProxyFactory factory,
        IntConsumer cleaner,
        int requestHash)
    {
        this.factory = factory;
        this.cleaner = cleaner;
        this.requestHash = requestHash;
        this.queuedRequestsByEtag = new HashMap<>();
        this.flushableResponses = new HashSet<>();
    }

    int requestHash()
    {
        return requestHash;
    }

    void setAuthorizationHeader(
        String authorizationHeader)
    {
        this.authorizationHeader = authorizationHeader;
    }

    String authorizationHeader()
    {
        return authorizationHeader;
    }

    String ifNoneMatchHeader()
    {
        return groupRequest != null ? groupRequest.request().ifNoneMatch : null;
    }

    void cacheEntry(
        DefaultCacheEntry cacheEntry)
    {
        cacheEntry.setSubscribers(flushableResponses.size());
        this.cacheEntry = cacheEntry;
    }

    void enqueue(
        HttpCacheProxyCacheableRequest request)
    {
        final Deque<HttpCacheProxyCacheableRequest> queuedRequests =
            queuedRequestsByEtag.computeIfAbsent(request.ifNoneMatch, e -> new LinkedList<>());

        final boolean added = queuedRequests.add(request);
        assert added;

        if (groupRequest == null)
        {
            doRequest(request, request.ifNoneMatch);
        }
        else if (!groupRequest.satisfiesRequest(request))
        {
            doRequest(request, null);
        }
    }

    void dequeue(
        HttpCacheProxyCacheableRequest request)
    {
        final String etag = request.ifNoneMatch;
        final Deque<HttpCacheProxyCacheableRequest> queuedRequests = queuedRequestsByEtag.get(etag);
        assert queuedRequests != null;

        boolean removed = queuedRequests.remove(request);
        assert removed;

        if (queuedRequests.isEmpty())
        {
            queuedRequestsByEtag.remove(etag);

            if (queuedRequestsByEtag.isEmpty())
            {
                cleaner.accept(requestHash);
            }
        }
    }

    void attach(
        HttpCacheProxyCachedResponse response)
    {
        flushableResponses.add(response);
    }

    void detach(
        HttpCacheProxyCachedResponse response)
    {
        flushableResponses.remove(response);

        if (flushableResponses.isEmpty() && !queuedRequestsByEtag.isEmpty())
        {
            flushNextRequest();
        }
    }

    private void flushNextRequest()
    {
        String nextIfNoneMatch = ifNoneMatch;
        Deque<HttpCacheProxyCacheableRequest> queuedRequests = queuedRequestsByEtag.get(nextIfNoneMatch);
        if (queuedRequests == null || queuedRequests.isEmpty())
        {
            Map.Entry<String, Deque<HttpCacheProxyCacheableRequest>> entry =
                    queuedRequestsByEtag.entrySet().iterator().next();
            nextIfNoneMatch = entry.getKey();
            queuedRequests = entry.getValue();
        }

        if (queuedRequests != null && !queuedRequests.isEmpty())
        {
            final HttpCacheProxyCacheableRequest nextRequest = queuedRequests.getFirst();
            doRequest(nextRequest, nextIfNoneMatch);
        }
    }

    void onCacheableRequestReset()
    {
        // TODO Auto-generated method stub
    }

    void onCacheableResponseUpdated(
        long traceId)
    {
        final Deque<HttpCacheProxyCacheableRequest> noEtagRequests = queuedRequestsByEtag.remove(null);
        if (noEtagRequests != null && !noEtagRequests.isEmpty())
        {
            for (HttpCacheProxyCacheableRequest noEtagRequest : noEtagRequests)
            {
                noEtagRequest.doCachedResponse(traceId);
            }
        }

        final String etag = cacheEntry.etag();
        final Deque<HttpCacheProxyCacheableRequest> etagRequests = queuedRequestsByEtag.remove(etag);
        if (etagRequests != null && !etagRequests.isEmpty())
        {
            for (HttpCacheProxyCacheableRequest etagRequest : etagRequests)
            {
                etagRequest.doNotModifiedResponse(traceId);
            }
        }

        flushableResponses.forEach(r -> r.doResponseFlush(traceId));
    }

    void onCacheableResponseAborted(
        long traceId)
    {
        for (HttpCacheProxyCachedResponse flushableResponse : flushableResponses)
        {
            flushableResponse.doResponseAbort(traceId);
        }

        factory.defaultCache.purge(requestHash);
    }

    public void onCacheEntryInvalidated(
        long traceId)
    {
        groupRequest.onCacheEntryInvalidated(traceId);
    }

    private void doRequest(
        HttpCacheProxyCacheableRequest request,
        String ifNoneMatch)
    {
        final long traceId = factory.supplyTraceId.getAsLong();

        if (groupRequest != null)
        {
            groupRequest.doResponseReset(traceId);
            groupRequest = null;
        }

        assert groupRequest == null;
        this.groupRequest = new HttpCacheProxyGroupRequest(factory, this, request);
        this.ifNoneMatch = ifNoneMatch;

        groupRequest.doRequest(traceId);
    }

    void onResponseNonCacheable(
        HttpCacheProxyCacheableRequest request)
    {
        assert groupRequest.request() == request;
        groupRequest = null;

        flushNextRequest();
    }
}
