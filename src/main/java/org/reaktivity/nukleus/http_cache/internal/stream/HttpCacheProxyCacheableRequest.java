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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

import java.time.Instant;
import java.util.concurrent.Future;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.satisfiedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

public final class HttpCacheProxyCacheableRequest
{
    private final HttpCacheProxyFactory factory;
    private final LongFunction<MessageConsumer> supplyReceiver;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;

    private MessageConsumer connectInitial;
    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;
    private final MessageConsumer signaler;

    private int requestSlot = NO_SLOT;
    private int requestHash;
    private boolean isRequestPurged;
    private String etag;
    private Future<?> preferWaitExpired;
    private int attempts;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory factory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptInitialId,
        long acceptReplyId,
        MessageConsumer connectInitial,
        MessageConsumer connectReply,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {
        this.factory = factory;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectInitial = connectInitial;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
        this.supplyReceiver = factory.router::supplyReceiver;
        this.signaler = supplyReceiver.apply(acceptInitialId);
    }

    public ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        return getRequestHeaders(requestHeaders, factory.requestBufferPool);
    }

    public ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO,
        BufferPool bp)
    {
        final MutableDirectBuffer buffer = bp.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    public LongUnaryOperator supplyInitialId()
    {
        return factory.supplyInitialId;
    }

    public LongUnaryOperator supplyReplyId()
    {
        return factory.supplyReplyId;
    }

    public String etag()
    {
        return etag;
    }

    public int requestHash()
    {
        return requestHash;
    }

    public boolean isRequestPurged()
    {
        return isRequestPurged;
    }

    public void purge()
    {
        if (requestSlot != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot);
            this.requestSlot = NO_SLOT;
        }
        this.isRequestPurged = true;
    }

    private boolean scheduleRequest(long retryAfter)
    {
        if (retryAfter <= 0L)
        {
            retryCacheableRequest();
        }
        else
        {
            long requestAt = Instant.now().plusMillis(retryAfter).toEpochMilli();
            this.factory.scheduler.accept(requestAt, this::retryCacheableRequest);
        }
        return true;
    }

    public MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        ListFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        DefaultCacheEntry cacheEntry = factory.defaultCache.supply(requestHash);
        String newEtag = getHeader(responseHeaders, ETAG);
        MessageConsumer newStream;

        if ((retry  &&
             attempts < 3) ||
            !this.factory.defaultCache.isUpdatedByResponseHeadersToRetry(this, responseHeaders))
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                connectReply,
                                                connectRouteId,
                                                connectReplyId,
                                                this::scheduleRequest);
            factory.router.setThrottle(acceptReplyId, cacheProxyRetryResponse::onResponseMessage);
            newStream = cacheProxyRetryResponse::onResponseMessage;

        }
        else if (cacheEntry.etag() != null &&
                 cacheEntry.etag().equals(newEtag) &&
                 cacheEntry.recentAuthorizationHeader() != null)
        {
            final HttpCacheProxyNotModifiedResponse notModifiedResponse =
                new HttpCacheProxyNotModifiedResponse(factory,
                                                      requestHash,
                                                      requestSlot,
                                                      acceptReply,
                                                      acceptRouteId,
                                                      acceptReplyId,
                                                      connectReply,
                                                      connectReplyId,
                                                      connectRouteId);
            factory.router.setThrottle(acceptReplyId, notModifiedResponse::onResponseMessage);
            newStream = notModifiedResponse::onResponseMessage;
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestHash,
                                                    requestSlot,
                                                    preferWaitExpired,
                                                    acceptReply,
                                                    acceptRouteId,
                                                    acceptInitialId,
                                                    acceptReplyId,
                                                    connectReply,
                                                    connectReplyId,
                                                    connectRouteId);
            factory.router.setThrottle(acceptReplyId, cacheableResponse::onResponseMessage);
            newStream = cacheableResponse::onResponseMessage;
        }
        else
        {
            final HttpCacheProxyNonCacheableResponse nonCacheableResponse =
                    new HttpCacheProxyNonCacheableResponse(factory,
                                                           connectReply,
                                                           connectRouteId,
                                                           connectReplyId,
                                                           acceptReply,
                                                           acceptRouteId,
                                                           acceptReplyId);
            factory.router.setThrottle(acceptReplyId, nonCacheableResponse::onResponseMessage);
            newStream = nonCacheableResponse::onResponseMessage;
            factory.defaultCache.serveNextPendingInitialRequest(this);
            purge();
        }

        return newStream;
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
            case ResetFW.TYPE_ID:
                factory.writer.doReset(acceptReply,
                                       acceptRouteId,
                                       acceptInitialId,
                                       factory.supplyTrace.getAsLong());
                break;
        }
    }

    void onRequestMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case BeginFW.TYPE_ID:
                final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = factory.endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            default:
                break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long authorization = begin.authorization();
        final short authorizationScope = authorizationScope(authorization);

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

        boolean stored = storeRequest(requestHeaders, factory.requestBufferPool);
        if (!stored)
        {
            send503RetryAfter();
            return;
        }

        // Should already be canonicalized in http / http2 nuklei
        final String requestURL = getRequestURL(requestHeaders);
        this.requestHash = RequestUtil.requestHash(authorizationScope, requestURL.hashCode());

        // count all requests
        factory.counters.requests.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [received request]\n",
                    currentTimeMillis(), acceptReplyId, getRequestURL(httpBeginFW.headers()));
        }

        boolean satisfiedByCache = satisfiedByCache(requestHeaders);
        if (satisfiedByCache)
        {
            factory.counters.requestsCacheable.getAsLong();
        }

        HttpHeaderFW ifNoneMatch = requestHeaders.matchFirst(h -> IF_NONE_MATCH.equals(h.name().asString()));
        if (ifNoneMatch != null)
        {
            etag = ifNoneMatch.value().asString();
        }

        long connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent initial request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(
            connectInitial,
            connectRouteId,
            connectInitialId,
            factory.supplyTrace.getAsLong(),
            builder -> requestHeaders.forEach(
                h ->  builder.item(item -> item.name(h.name()).value(h.value()))));

        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
        schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
    }

    private void onData(
        final DataFW data)
    {
        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                data.trace(),
                                data.sizeof(),
                                data.padding(),
                                data.groupId());
    }

    private void onEnd(
        final EndFW end)
    {
        final long traceId = end.trace();
        factory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, traceId);
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
        factory.defaultCache.removePendingInitialRequest(this);
        purge();
    }

    private void onWindow(
        final WindowFW window)
    {
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();
        final long traceId = window.trace();
        factory.writer.doWindow(acceptReply, acceptRouteId, acceptInitialId, traceId, credit, padding, groupId);
    }

    private void onReset(
        final ResetFW reset)
    {
        final long traceId = reset.trace();
        factory.writer.doReset(acceptReply, acceptRouteId, acceptInitialId, traceId);

        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
        }

        this.factory.defaultCache.removePendingInitialRequest(this);
    }

    private void schedulePreferWaitIfNoneMatchIfNecessary(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = this.factory.executor.schedule(preferWait,
                                                                   SECONDS,
                                                                   acceptRouteId,
                                                                   this.acceptReplyId,
                                                                   REQUEST_EXPIRED_SIGNAL);
            }
        }
    }

    private boolean storeRequest(
        final ListFW<HttpHeaderFW> headers,
        final BufferPool bufferPool)
    {
        this.requestSlot = bufferPool.acquire(acceptInitialId);
        if (requestSlot == NO_SLOT)
        {
            return false;
        }
        MutableDirectBuffer requestCacheBuffer = bufferPool.buffer(requestSlot);
        requestCacheBuffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
        return true;
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId, "503");
        }

        factory.writer.doHttpResponse(acceptReply,
                                      acceptRouteId,
                                      acceptReplyId,
                                      factory.supplyTrace.getAsLong(), e ->
                                            e.item(h -> h.name(STATUS).value("503"))
                                             .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(acceptReply,
                                 acceptRouteId,
                                 acceptReplyId,
                                 factory.supplyTrace.getAsLong());

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();
    }

    private void retryCacheableRequest()
    {
        if (isRequestPurged())
        {
            return;
        }

        incAttempts();

        connectInitialId = this.factory.supplyInitialId.applyAsLong(connectRouteId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectInitial = this.factory.router.supplyReceiver(connectInitialId);

        factory.correlations.put(connectReplyId, this::newResponse);
        ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders(factory.requestHeadersRO);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTrace.getAsLong(),
                                           builder ->
                                           {
                                               requestHeaders.forEach(
                                                   h -> builder.item(item -> item.name(h.name()).value(h.value())));
                                           });
        factory.writer.doHttpEnd(connectInitial,
                                 connectRouteId,
                                 connectInitialId,
                                 factory.supplyTrace.getAsLong());
        factory.counters.requestsRetry.getAsLong();
    }

    private void incAttempts()
    {
        attempts++;
    }
}
