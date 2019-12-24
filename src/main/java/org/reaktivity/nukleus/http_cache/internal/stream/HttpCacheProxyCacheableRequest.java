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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.PREFER_WAIT_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.concurrent.Future;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;

final class HttpCacheProxyCacheableRequest
{
    private static final StringFW HEADER_NAME_STATUS = new StringFW(":status");
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
    private String prefer;
    private Future<?> preferWaitExpired;
    private boolean promiseNextPollRequest;

    private int headersSlot = NO_SLOT;

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

    void doCachedResponse(
        long traceId)
    {
        final int requestHash = requestGroup.requestHash();
        final DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestHash);
        final HttpCacheProxyCachedResponse response = new HttpCacheProxyCachedResponse(
            factory, reply, routeId, replyId, authorization,
            cacheEntry, promiseNextPollRequest, requestGroup::detach);

        response.doResponseBegin(traceId);
        requestGroup.attach(response);
        cleanupRequestHeadersIfNecessary();
    }

    void doNotModifiedResponse(
        long traceId)
    {
        factory.defaultCache.send304(ifNoneMatch,
                                     prefer,
                                     reply,
                                     routeId,
                                     replyId);
        factory.counters.responses.getAsLong();
        factory.counters.responsesNotModified.getAsLong();
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
        return new HttpCacheProxyRelayedResponse(factory, reply, routeId, replyId, sender, senderRouteId, senderReplyId);
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
        final ArrayFW<HttpHeaderFW> headers = httpBeginFW.headers();

        authorization = begin.authorization();
        promiseNextPollRequest = headers.anyMatch(HAS_EMULATED_PROTOCOL_STACK);

        factory.writer.doWindow(reply,
                routeId,
                initialId,
                traceId,
                0L,
                factory.initialWindowSize,
                0);

        assert headersSlot == NO_SLOT;
        headersSlot = factory.headersPool.acquire(initialId);
        if (headersSlot == NO_SLOT)
        {
            doResponseBeginEnd503RetryAfter();
        }
        else
        {
            final MutableDirectBuffer headersBuffer = factory.headersPool.buffer(headersSlot);
            final ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> newHeaders =
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

            final int requestHash = requestGroup.requestHash();
            final DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestHash);
            vary = cacheEntry != null && cacheEntry.getVaryBy() != null ?
                getHeader(headers, cacheEntry.getVaryBy()) : null;

            doResponseTimeoutIfNecessary(headers);
        }
    }

    private void onRequestData(
        final DataFW data)
    {
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final int padding = 0;

        factory.writer.doWindow(reply,
                                routeId,
                                initialId,
                                traceId,
                                budgetId,
                                reserved,
                                padding);
    }

    private void onRequestEnd(
        final EndFW end)
    {
        if (headersSlot != NO_SLOT)
        {
            requestGroup.enqueue(this);
        }
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        // note: not enqueued
        cleanupRequest();
        cleanupRequestTimeoutIfNecessary();
    }

    private void doResponseTimeoutIfNecessary(
        ArrayFW<HttpHeaderFW> requestHeaders)
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

    ArrayFW<HttpHeaderFW> getHeaders()
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

    private void onResponseReset(
        ResetFW reset)
    {
        cleanupRequest();
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
        factory.counters.responses.getAsLong();
        factory.defaultCache.send304(ifNoneMatch,
                                     prefer,
                                     reply,
                                     routeId,
                                     replyId);

        requestGroup.dequeue(this);
    }

    private void doResponseBeginEnd503RetryAfter()
    {
        factory.writer.doHttpResponse(
            reply,
            routeId,
            replyId,
            factory.supplyTraceId.getAsLong(),
            e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                  .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(
            reply,
            routeId,
            replyId,
            factory.supplyTraceId.getAsLong());

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();
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
