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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_INVALIDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.GROUP_REQUEST_RETRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.RETRY_AFTER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyGroupRequest
{
    private static final StringFW HEADER_NAME_STATUS = new StringFW(":status");
    private static final String16FW HEADER_VALUE_STATUS_503 = new String16FW(SERVICE_UNAVAILABLE_503);
    private final HttpBeginExFW.Builder beginExRW = new HttpBeginExFW.Builder();

    private final int httpTypeId;
    private final MutableInteger requestSlot;

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final MessageConsumer initial;

    private MessageConsumer connectInitial;
    private long connectReplyId;
    private long connectInitialId;

    private Future<?> retryRequest;
    private String requestURL;
    private long initialId;
    private long replyId;
    private long routeId;
    private int attempts;
    private int state;
    private short authScope;

    HttpCacheProxyGroupRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        MessageConsumer initial)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.initial = initial;
        this.requestSlot =  new MutableInteger(NO_SLOT);
        this.httpTypeId = factory.supplyTypeId.applyAsInt("http");
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        if (!requestGroup.isRequestStillQueued(initialId))
        {
            return null;
        }

        MessageConsumer newStream = null;
        ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);

        if ((retry && attempts < 3) ||
            (factory.defaultCache.checkToRetry(getRequestHeaders(),
                                               responseHeaders,
                                               requestGroup.getEtag(),
                                               requestGroup.getRequestHash())))
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                requestGroup.getRequestHash(),
                                                connectInitial,
                                                routeId,
                                                connectReplyId,
                                                this::scheduleRequest);
            newStream = cacheProxyRetryResponse::onResponseMessage;
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestGroup,
                                                    requestURL,
                                                    requestSlot,
                                                    authScope,
                                                    connectInitial,
                                                    connectReplyId,
                                                    routeId,
                                                    this::scheduleRequest);
            newStream = cacheableResponse::onResponseMessage;
        }
        else
        {
            Function<HttpBeginExFW, MessageConsumer> responseFactory = factory.correlations.remove(replyId);
            if (responseFactory != null)
            {
                MessageConsumer newResponse = responseFactory.apply(beginEx);
                newStream = onResponseMessage(newResponse);
            }
        }

        return newStream;
    }

    private MessageConsumer onResponseMessage(
        MessageConsumer newResponse)
    {
        return (t, b, i, l) ->
        {
            factory.writeBuffer.putBytes(0, b, i, l);
            switch (t)
            {
            case BeginFW.TYPE_ID:
            case DataFW.TYPE_ID:
                factory.writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, replyId);
                newResponse.accept(t, factory.writeBuffer, 0, l);
                break;
            case EndFW.TYPE_ID:
            case AbortFW.TYPE_ID:
                factory.writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, replyId);
                newResponse.accept(t, factory.writeBuffer, 0, l);
                factory.router.clearThrottle(replyId);
                break;
            case ResetFW.TYPE_ID:
            case WindowFW.TYPE_ID:
            case SignalFW.TYPE_ID:
                factory.writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, initialId);
                newResponse.accept(t, factory.writeBuffer, 0, l);
                break;
            default:
                break;
            }
        };
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
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onSignal(signal);
            break;
        default:
            break;
        }
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
            onRequestReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onRequestWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onRequestSignal(signal);
            break;
        default:
            break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();
        requestURL = getRequestURL(requestHeaders);
        routeId = begin.routeId();
        initialId = begin.streamId();
        replyId = factory.supplyReplyId.applyAsLong(initialId);
        factory.router.setThrottle(replyId, this::onResponseMessage);
        long authorization = begin.authorization();
        authScope = authorizationScope(authorization);

        boolean stored = storeRequest(requestHeaders);
        if (!stored)
        {
            final long traceId = begin.traceId();
            send503RetryAfter(traceId);
            cleanupRequestIfNecessary();
            return;
        }
        doHttpBegin(requestHeaders);
        state = RequestState.openingInitial(state);
    }

    private void onData(
        final DataFW data)
    {
        //NOOP
    }

    private void onEnd(
        final EndFW end)
    {
        state = RequestState.closeInitial(state);
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.traceId();
        factory.writer.doAbort(connectInitial, routeId, connectInitialId, traceId);
        factory.writer.doReset(connectInitial, routeId, connectReplyId, traceId);
        cleanupRequestIfNecessary();
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        state = RequestState.closeInitial(state);
        factory.writer.doHttpEnd(connectInitial,
                                 routeId,
                                 connectInitialId,
                                 window.traceId());
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        final long traceId = reset.traceId();
        if (RequestState.initialClosed(state))
        {
            send503RetryAfter(traceId);
        }
        else
        {
            factory.writer.doReset(initial, routeId, initialId, traceId);
            factory.router.clearThrottle(connectReplyId);
        }

        cleanupRequestIfNecessary();
    }

    private void onRequestSignal(
        SignalFW signal)
    {
        final int signalId = signal.signalId();

        if (signalId == GROUP_REQUEST_RETRY_SIGNAL)
        {
            retryCacheableRequest();
        }
    }

    private void onWindow(
        final WindowFW window)
    {
        state = RequestState.openInitial(state);
        factory.writer.doWindow(connectInitial,
                                routeId,
                                connectReplyId,
                                window.traceId(),
                                window.budgetId(),
                                window.credit(),
                                window.padding());
    }

    private void onReset(
        final ResetFW reset)
    {
        factory.writer.doReset(connectInitial,
                               routeId,
                               connectReplyId,
                               reset.traceId());
        cleanupRequestIfNecessary();
    }

    private void onSignal(
        final SignalFW signal)
    {
        final int signalId = signal.signalId();

        if (signalId == CACHE_ENTRY_INVALIDATED_SIGNAL)
        {
            onRequestCacheEntryInvalidatedSignal();
        }
    }

    private void onRequestCacheEntryInvalidatedSignal()
    {
        if (retryRequest != null)
        {
            retryRequest.cancel(true);
            retryCacheableRequest();
        }
    }

    private boolean scheduleRequest(
        long retryAfter)
    {
        if (retryAfter <= 0L)
        {
            retryCacheableRequest();
        }
        else
        {
            retryRequest = factory.executor.schedule(retryAfter,
                                                     MILLISECONDS,
                                                     routeId,
                                                     replyId,
                                                     GROUP_REQUEST_RETRY_SIGNAL);
        }
        return true;
    }

    private void retryCacheableRequest()
    {
        incAttempts();
        factory.counters.requestsRetry.getAsLong();
        doHttpBegin(getRequestHeaders());
    }

    private boolean storeRequest(
        final ArrayFW<HttpHeaderFW> headers)
    {
        assert requestSlot.value == NO_SLOT;
        int newRequestSlot = factory.requestBufferPool.acquire(replyId);
        if (newRequestSlot == NO_SLOT)
        {
            return false;
        }
        requestSlot.value = newRequestSlot;
        MutableDirectBuffer requestCacheBuffer = factory.requestBufferPool.buffer(requestSlot.value);
        requestCacheBuffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
        return true;
    }

    private ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot.value);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    private void incAttempts()
    {
        attempts++;
    }

    private Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutateRequestHeaders(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        return (ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder) ->
        {
            requestHeaders.forEach(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                if (!CONTENT_LENGTH.equals(name) &&
                    !AUTHORIZATION.equals(name) &&
                    !IF_NONE_MATCH.equals(name))
                {
                    builder.item(item -> item.name(name).value(value));
                }
            });

            final String authorizationToken = requestGroup.getRecentAuthorizationToken();
            if (authorizationToken != null)
            {
                builder.item(item -> item.name(AUTHORIZATION).value(authorizationToken));
            }
            if (requestGroup.getEtag() != null)
            {
                builder.item(item -> item.name(IF_NONE_MATCH).value(requestGroup.getEtag()));
            }
        };
    }

    private void doHttpBegin(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        connectInitialId = factory.supplyInitialId.applyAsLong(routeId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectInitial = factory.router.supplyReceiver(connectInitialId);
        factory.correlations.put(connectReplyId, this::newResponse);

        factory.writer.doHttpRequest(connectInitial,
                                     routeId,
                                     connectInitialId,
                                     factory.supplyTraceId.getAsLong(),
                                     0L,
                                     mutateRequestHeaders(requestHeaders));
        factory.router.setThrottle(connectInitialId, this::onResponseMessage);
        retryRequest = null;
    }

    private void send503RetryAfter(
        long traceId)
    {
        Function<HttpBeginExFW, MessageConsumer> responseFactory = factory.correlations.remove(replyId);
        if (responseFactory != null)
        {
            beginExRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity());

            final Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
                e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                      .item(h -> h.name(RETRY_AFTER).value("0"));

            beginExRW.typeId(httpTypeId).headers(mutator);

            // count retry responses
            factory.counters.responsesRetry.getAsLong();

            MessageConsumer newResponse = responseFactory.apply(beginExRW.build());

            factory.writer.doHttpResponse(newResponse,
                                          routeId,
                                          replyId,
                                          traceId,
                                          mutator);

            factory.writer.doHttpEnd(newResponse,
                                     routeId,
                                     replyId,
                                     traceId);
        }
    }

    private void cleanupRequestIfNecessary()
    {
        factory.correlations.remove(connectReplyId);
        factory.correlations.remove(replyId);
        factory.router.clearThrottle(replyId);
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }

        if (retryRequest != null)
        {
            retryRequest.cancel(true);
            retryRequest = null;
        }
    }

    private static final class RequestState
    {
        private static final int INITIAL_OPENING = 0x10;
        private static final int INITIAL_OPENED = 0x20;
        private static final int INITIAL_CLOSED = 0x40;

        static int openingInitial(
            int state)
        {
            return state | INITIAL_OPENING;
        }

        static int openInitial(
            int state)
        {
            return openingInitial(state) | INITIAL_OPENED;
        }

        static int closeInitial(
            int state)
        {
            return state | INITIAL_CLOSED;
        }

        static boolean initialClosed(
            int state)
        {
            return (state & INITIAL_CLOSED) != 0;
        }
    }
}
