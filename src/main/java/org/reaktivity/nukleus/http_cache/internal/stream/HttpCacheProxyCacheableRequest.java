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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.hasMaxAgeZero;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.PREFER_WAIT_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.time.Instant;
import java.util.concurrent.Future;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.String8FW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyCacheableRequest
{
    private static final String8FW HEADER_NAME_STATUS = new String8FW(":status");
    private static final String16FW HEADER_VALUE_STATUS_503 = new String16FW(SERVICE_UNAVAILABLE_503);

    private final HttpCacheProxyFactory factory;
    final HttpProxyCacheableRequestGroup requestGroup;

    private final MessageConsumer reply;
    private final long routeId;
    private final long initialId;
    final long replyId;
    final long resolveId;
    long authorization;

    String ifNoneMatch;
    String vary;
    String prefer;
    boolean maxAgeZero;

    private Future<?> preferWaitExpired;
    private boolean promiseNextPollRequest;

    private int headersSlot = NO_SLOT;
    private long replySeq;
    private long replyAck;
    private int replyMax;
    private int replyPad;
    private long replyBudgetId;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        MessageConsumer reply,
        long routeId,
        long initialId,
        long resolveId)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.reply = reply;
        this.routeId = routeId;
        this.initialId = initialId;
        this.resolveId = resolveId;
        this.replyId = factory.supplyReplyId.applyAsLong(initialId);
    }

    void onQueuedRequestSent()
    {
        cleanupRequestHeadersIfNecessary();
    }

    void doCachedResponse(
        Instant now,
        long traceId)
    {
        final int requestHash = requestGroup.requestHash();
        final HttpCacheProxyCachedResponse response = new HttpCacheProxyCachedResponse(
            factory,
            reply,
            routeId,
            replyId,
            authorization,
            replyBudgetId,
            replySeq,
            replyAck,
            replyMax,
            replyPad,
            requestHash,
            promiseNextPollRequest,
            requestGroup::detach);

        requestGroup.attach(response);
        response.doResponseBegin(now, traceId);
        cleanupRequestHeadersIfNecessary();
    }

    void doNotModifiedResponse(
        long traceId)
    {
        factory.defaultCache.send304(requestGroup.requestHash(),
                                     ifNoneMatch,
                                     prefer,
                                     reply,
                                     routeId,
                                     replyId,
                                     replySeq,
                                     replyAck,
                                     replyMax,
                                     traceId,
                                     authorization,
                                     promiseNextPollRequest);
        factory.counters.responses.getAsLong();
        factory.counters.responsesNotModified.getAsLong();
        factory.counters.responsesCached.getAsLong();
        cleanupRequestHeadersIfNecessary();
    }

    void do503RetryResponse(
        long traceId)
    {
        factory.writer.doHttpResponse(
            reply,
            routeId,
            replyId,
            replySeq,
            replyAck,
            replyMax,
            traceId,
            e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                  .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(
            reply,
            routeId,
            replyId,
            replySeq,
            replyAck,
            replyMax,
            traceId);

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();

        cleanupRequestHeadersIfNecessary();
    }

    HttpCacheProxyRelayedResponse newRelayedResponse(
        MessageConsumer sender,
        long senderRouteId,
        long senderReplyId)
    {
        factory.counters.responses.getAsLong();
        requestGroup.dequeue(this);
        cleanupRequestHeadersIfNecessary();
        return new HttpCacheProxyRelayedResponse(
            factory,
            reply,
            routeId,
            replyId,
            sender,
            senderRouteId,
            senderReplyId,
            prefer,
            replyBudgetId,
            replySeq,
            replyAck,
            replyMax,
            replyPad);
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
            onRequestBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onRequestData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onRequestEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onRequestAbort(abort);
            break;
        }
    }

    private void onRequestBegin(
        BeginFW begin)
    {
        final long traceId = begin.traceId();
        final OctetsFW extension = begin.extension();

        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final Array32FW<HttpHeaderFW> headers = httpBeginFW.headers();

        authorization = begin.authorization();
        promiseNextPollRequest = headers.anyMatch(HAS_EMULATED_PROTOCOL_STACK);

        factory.writer.doWindow(reply,
                                routeId,
                                initialId,
                                0,
                                0,
                                factory.initialWindowSize,
                                traceId,
                                0L,
                                0);

        assert headersSlot == NO_SLOT;
        headersSlot = factory.headersPool.acquire(initialId);
        if (headersSlot == NO_SLOT)
        {
            do503RetryResponse(traceId);
        }
        else
        {
            final MutableDirectBuffer headersBuffer = factory.headersPool.buffer(headersSlot);
            final Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> newHeaders =
                factory.httpHeadersRW.wrap(headersBuffer, 0, headersBuffer.capacity());
            headers.forEach(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                if (!CONTENT_LENGTH.equals(name))
                {
                    newHeaders.item(item -> item.name(name).value(value));
                }
            });
            newHeaders.build();

            ifNoneMatch = getHeader(headers, IF_NONE_MATCH);
            prefer = getHeader(headers, PREFER);
            maxAgeZero = hasMaxAgeZero(headers);


            final int requestHash = requestGroup.requestHash();
            final DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestHash);
            vary = cacheEntry != null && cacheEntry.getVaryBy() != null ?
                getHeader(headers, cacheEntry.getVaryBy()) : null;

            doResponseTimeoutIfNecessary(headers);
        }

        factory.counters.requestsCacheable.getAsLong();
    }

    private void onRequestData(
        final DataFW data)
    {
        final long sequence = data.sequence();
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final int padding = 0;

        factory.writer.doWindow(reply,
                                routeId,
                                initialId,
                                sequence + reserved,
                                sequence + reserved,
                                factory.initialWindowSize,
                                traceId,
                                budgetId,
                                padding);
    }

    private void onRequestEnd(
        final EndFW end)
    {
        assert  headersSlot != NO_SLOT;

        final DefaultCacheEntry entry = factory.defaultCache.get(requestGroup.requestHash());
        final Array32FW<HttpHeaderFW> headers = getHeaders();
        final short authScope = authorizationScope(authorization);
        final boolean isCacheEntryUpToDate = isCacheEntryUpdatedToBeServed(headers, authScope, entry);
        final boolean canBeCachedServed =
            factory.defaultCache.matchCacheableRequest(headers, authScope, requestGroup.requestHash());

        if (canBeCachedServed || isCacheEntryUpToDate)
        {
            final long traceId = end.traceId();

            final long replyId = factory.supplyReplyId.applyAsLong(initialId);
            final HttpCacheProxyCachedResponse response = new HttpCacheProxyCachedResponse(
                factory,
                reply,
                routeId,
                replyId,
                authorization,
                replyBudgetId,
                replySeq,
                replyAck,
                replyMax,
                replyPad,
                requestGroup.requestHash(),
                promiseNextPollRequest,
                requestGroup::detach);
            final Instant now = Instant.now();
            requestGroup.attach(response);
            response.doResponseBegin(now, traceId);
            cleanupRequestHeadersIfNecessary();
            cleanupRequestTimeoutIfNecessary();
        }
        else
        {
            requestGroup.enqueue(this);
        }
    }

    private boolean isCacheEntryUpdatedToBeServed(
        Array32FW<HttpHeaderFW> headers,
        short authScope,
        DefaultCacheEntry cacheEntry)
    {
        return cacheEntry != null &&
               hasMaxAgeZero(headers) &&
               requestGroup.hasQueuedRequests() &&
               (ifNoneMatch == null || ifNoneMatch.equals(requestGroup.ifNoneMatchHeader())) &&
               cacheEntry.canServeRequest(headers, authScope);
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        // note: not enqueued
        cleanupRequest();
        cleanupRequestTimeoutIfNecessary();
    }

    private void doResponseTimeoutIfNecessary(
        Array32FW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            final int preferWait = Math.min(getPreferWait(requestHeaders), factory.preferWaitMaximum);
            if (preferWait > 0)
            {
                preferWaitExpired = factory.executor.schedule(preferWait,
                                                              SECONDS,
                                                              routeId,
                                                              replyId,
                                                              PREFER_WAIT_EXPIRED_SIGNAL);
            }
        }
    }

    Array32FW<HttpHeaderFW> getHeaders()
    {
        final MutableDirectBuffer buffer = factory.headersPool.buffer(headersSlot);
        return factory.httpHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onResponseWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onResponseReset(reset);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onResponseSignal(signal);
            break;
        default:
            break;
        }
    }

    private void onResponseWindow(
        WindowFW window)
    {
        replyBudgetId = window.budgetId();
        replySeq = window.sequence();
        replyAck = window.acknowledge();
        replyMax = window.maximum();
        replyPad = window.padding();
    }

    private void onResponseReset(
        ResetFW reset)
    {
        final long traceId = reset.traceId();
        requestGroup.dequeue(this);
        cleanupRequest();
        requestGroup.onResponseAbandoned(traceId);
    }

    private void onResponseSignal(
        SignalFW signal)
    {
        final long traceId = signal.traceId();
        final int signalId = signal.signalId();

        switch (signalId)
        {
        case PREFER_WAIT_EXPIRED_SIGNAL:
            onResponseSignalPreferWaitExpired(traceId);
            break;
        }
    }

    private void onResponseSignalPreferWaitExpired(
        long traceId)
    {
        factory.defaultCache.send304(requestGroup.requestHash(),
                                     ifNoneMatch,
                                     prefer,
                                     reply,
                                     routeId,
                                     replyId,
                                     replySeq,
                                     replyAck,
                                     replyMax,
                                     traceId,
                                     authorization,
                                     promiseNextPollRequest);

        requestGroup.dequeue(this);
        requestGroup.onResponseAbandoned(traceId);
        cleanupRequest();

        factory.counters.responses.getAsLong();
    }

    private void cleanupRequestTimeoutIfNecessary()
    {
        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
            preferWaitExpired = null;
        }
    }

    private void cleanupRequest()
    {
        cleanupRequestHeadersIfNecessary();

        factory.router.clearThrottle(replyId);
    }

    private void cleanupRequestHeadersIfNecessary()
    {
        if (headersSlot != NO_SLOT)
        {
            factory.headersPool.release(headersSlot);
            headersSlot = NO_SLOT;
        }
    }
}
