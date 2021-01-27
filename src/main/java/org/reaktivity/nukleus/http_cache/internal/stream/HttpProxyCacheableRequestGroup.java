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
package org.reaktivity.nukleus.http_cache.internal.stream;

import java.time.Instant;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.IntConsumer;

import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;

public final class HttpProxyCacheableRequestGroup
{
    private final HttpCacheProxyFactory factory;
    private final IntConsumer cleaner;
    private final int requestHash;
    private final Deque<HttpCacheProxyCacheableRequest> queuedRequests;
    private final Set<HttpCacheProxyCachedResponse> attachedResponses;
    private final Set<HttpCacheProxyCachedResponse> detachedResponses;

    private String authorizationHeader;
    private HttpCacheProxyGroupRequest groupRequest;
    private DefaultCacheEntry cacheEntry;
    private boolean groupRequestDeleted;

    public void onCacheEntryInvalidated(
        long traceId)
    {
        if (groupRequest != null)
        {
            groupRequest.doRetryRequestImmediatelyIfPending(traceId);
        }
    }

    HttpProxyCacheableRequestGroup(
        HttpCacheProxyFactory factory,
        IntConsumer cleaner,
        int requestHash)
    {
        this.factory = factory;
        this.cleaner = cleaner;
        this.requestHash = requestHash;
        this.queuedRequests = new LinkedList<>();
        this.attachedResponses = new HashSet<>();
        this.detachedResponses = new HashSet<>();
    }

    int requestHash()
    {
        return requestHash;
    }

    void authorizationHeader(
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
        this.cacheEntry = cacheEntry;
    }

    void enqueue(
        HttpCacheProxyCacheableRequest request)
    {
        final boolean added = queuedRequests.add(request);
        assert added;

        if (groupRequest == null || !groupRequest.canDeferRequest(request))
        {
            doRequest(request);
        }
        else if (cacheEntry != null && !attachedResponses.isEmpty())
        {
            final String etag = cacheEntry.etag();
            final boolean notModified = etag != null && etag.equals(request.ifNoneMatch);
            final long traceId = factory.supplyTraceId.getAsLong();
            if (notModified)
            {
                request.doNotModifiedResponse(traceId);
            }
            else
            {
                request.doCachedResponse(Instant.now(), traceId);
            }
            queuedRequests.remove(request);
        }
    }

    void dequeue(
        HttpCacheProxyCacheableRequest request)
    {
        boolean removed = queuedRequests.remove(request);
        assert removed;

        cleanupRequestGroupIfNecessary();
    }

    private void cleanupRequestGroupIfNecessary()
    {
        if (!hasQueuedRequests() && !hasAttachedResponses() && !groupRequestDeleted)
        {
            cleaner.accept(requestHash);
            factory.counters.requestGroups.accept(-1);
            groupRequestDeleted = true;
        }
    }

    void attach(
        HttpCacheProxyCachedResponse response)
    {
        attachedResponses.add(response);
    }

    void detach(
        HttpCacheProxyCachedResponse response)
    {
        assert attachedResponses.contains(response);
        detachedResponses.add(response);
        cleanupRequestGroupIfNecessary();
    }

    void onResponseAbandoned(
        long traceId)
    {
        if (groupRequest != null &&
            !hasQueuedRequests() &&
            !hasAttachedResponses())
        {
            groupRequest.doResponseReset(traceId);
            groupRequest = null;
        }
    }

    void onGroupRequestReset(
        long traceId)
    {
        queuedRequests.forEach(r -> r.do503RetryResponse(traceId));
        queuedRequests.clear();
        cleanupRequestGroupIfNecessary();
    }

    void onGroupResponseBegin(
        Instant now,
        long traceId)
    {
        final String etag = cacheEntry.etag();
        for (HttpCacheProxyCacheableRequest queuedRequest : queuedRequests)
        {
            final boolean notModified = etag != null && etag.equals(queuedRequest.ifNoneMatch);
            if (notModified)
            {
                queuedRequest.doNotModifiedResponse(traceId);
            }
            else
            {
                queuedRequest.doCachedResponse(now, traceId);
            }
        }
        queuedRequests.clear();
    }

    void onGroupResponseData(
        long traceId)
    {
        attachedResponses.removeIf(detachedResponses::contains);
        detachedResponses.clear();
        attachedResponses.forEach(response -> response.doResponseFlush(traceId));
        cleanupRequestGroupIfNecessary();
    }

    void onGroupResponseAbort(
        long traceId)
    {
        queuedRequests.forEach(r -> r.do503RetryResponse(traceId));
        queuedRequests.clear();

        attachedResponses.forEach(r -> r.doResponseAbort(traceId));
        attachedResponses.clear();
        detachedResponses.clear();

        cleanupRequestGroupIfNecessary();
    }

    void onGroupRequestEnd(
        HttpCacheProxyCacheableRequest request)
    {
        assert groupRequest.request() == request;
        groupRequest = null;

        flushNextRequest();
    }

    private void doRequest(
        HttpCacheProxyCacheableRequest request)
    {
        final long traceId = factory.supplyTraceId.getAsLong();

        if (groupRequest != null)
        {
            groupRequest.doResponseReset(traceId);
            groupRequest = null;
        }

        this.groupRequest = new HttpCacheProxyGroupRequest(factory, this, request);

        groupRequest.doRequest(traceId);
        request.onQueuedRequestSent();
    }

    private void flushNextRequest()
    {
        if (!queuedRequests.isEmpty())
        {
            final HttpCacheProxyCacheableRequest nextRequest = queuedRequests.getFirst();
            doRequest(nextRequest);
        }
    }

    boolean hasQueuedRequests()
    {
        return !queuedRequests.isEmpty();
    }

    boolean hasAttachedResponses()
    {
        return attachedResponses.size() != detachedResponses.size();
    }

    public boolean isQueuedRequest(
        HttpCacheProxyCacheableRequest request)
    {
        return queuedRequests.contains(request);
    }
}
