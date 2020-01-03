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

    private String authorizationHeader;
    private HttpCacheProxyGroupRequest groupRequest;
    private DefaultCacheEntry cacheEntry;

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
        cacheEntry.setSubscribers(attachedResponses.size());
        this.cacheEntry = cacheEntry;
    }

    void enqueue(
        HttpCacheProxyCacheableRequest request)
    {
        System.out.printf("[enqueue] [%d] [0x%16x] \n", requestHash, request.replyId);
        final boolean added = queuedRequests.add(request);
        assert added;

        if (groupRequest == null || !groupRequest.canDeferRequest(request))
        {
            doRequest(request);
        }
        else if (!attachedResponses.isEmpty())
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
        System.out.printf("[dequeue] [%d] [0x%16x] \n", requestHash, request.replyId);
        boolean removed = queuedRequests.remove(request);
        assert removed;

        if (queuedRequests.isEmpty())
        {
            cleaner.accept(requestHash);
        }
    }

    void attach(
        HttpCacheProxyCachedResponse response)
    {
        System.out.printf("[attach] [%d] [0x%16x] \n", requestHash, response.replyId);
        attachedResponses.add(response);
    }

    void detach(
        HttpCacheProxyCachedResponse response)
    {
        System.out.printf("[detach] [%d] [0x%16x] \n", requestHash, response.replyId);
        attachedResponses.remove(response);
    }

    void onResponseAbandoned(
        long traceId)
    {
        System.out.printf("[onResponseAbandoned] [%d] \n", requestHash);
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
        System.out.printf("[onGroupRequestReset] [%d] \n", requestHash);
        queuedRequests.forEach(r -> r.do503RetryResponse(traceId));
        queuedRequests.clear();
    }

    void onGroupResponseBegin(
        Instant now,
        long traceId)
    {
        System.out.printf("[onGroupResponseBegin] [%d] \n", requestHash);
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
        System.out.printf("[onGroupResponseData] [%d] \n", requestHash);
        attachedResponses.forEach(r -> r.doResponseFlush(traceId));
    }

    void onGroupResponseAbort(
        long traceId)
    {
        System.out.printf("[onGroupResponseAbort] [%d] \n", requestHash);
        queuedRequests.forEach(r -> r.do503RetryResponse(traceId));
        queuedRequests.clear();

        attachedResponses.forEach(r -> r.doResponseAbort(traceId));
        attachedResponses.clear();
    }

    void onGroupRequestEnd(
        HttpCacheProxyCacheableRequest request)
    {
        System.out.printf("[onGroupRequestEnd] [%d] \n", requestHash);
        assert groupRequest.request() == request;
        groupRequest = null;
        attachedResponses.clear();

        flushNextRequest();
    }

    private void doRequest(
        HttpCacheProxyCacheableRequest request)
    {
        System.out.printf("[doRequest] [%d] [0x%16x] \n", requestHash, request.replyId);
        final long traceId = factory.supplyTraceId.getAsLong();

        if (groupRequest != null)
        {
            groupRequest.doResponseReset(traceId);
            groupRequest = null;
        }

        assert groupRequest == null;
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
        return !attachedResponses.isEmpty();
    }
}
