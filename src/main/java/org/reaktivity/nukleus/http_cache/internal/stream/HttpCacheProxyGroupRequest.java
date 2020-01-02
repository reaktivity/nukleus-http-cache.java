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
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.GROUP_REQUEST_RETRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyGroupRequest
{
    private static final LongConsumer NOOP_RESET_HANDLER = t -> {};

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final HttpCacheProxyCacheableRequest request;
    private final long routeId;
    private final long notifyId;

    private MessageConsumer initial;
    private long initialId;
    private long replyId;
    private int state;

    private int attempts;
    private int headersSlot = NO_SLOT;

    private Future<?> retryRequest;
    private LongConsumer resetHandler = NOOP_RESET_HANDLER;

    HttpCacheProxyGroupRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        HttpCacheProxyCacheableRequest request)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.request = request;
        this.routeId = request.resolveId;
        this.notifyId = factory.supplyInitialId.applyAsLong(routeId);
    }

    HttpCacheProxyCacheableRequest request()
    {
        return request;
    }

    void doRequest(
        long traceId)
    {
        assert headersSlot == NO_SLOT;
        headersSlot = factory.headersPool.acquire(replyId);
        if (headersSlot == NO_SLOT)
        {
            requestGroup.onGroupRequestReset(request, traceId);
            cleanupRequestIfNecessary();
        }
        else
        {
            final ArrayFW<HttpHeaderFW> headers = request.getHeaders();

            // TODO: override if-none-match from requestGroup.ifNoneMatch

            final MutableDirectBuffer headersBuffer = factory.headersPool.buffer(headersSlot);
            headersBuffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());

            doRequestAttempt(traceId);
        }
    }

    boolean canDeferRequest(
        HttpCacheProxyCacheableRequest newRequest)
    {
        return request.prefer == null || request.ifNoneMatch == null || request.maxAgeZero ||
                (request.ifNoneMatch != null && request.ifNoneMatch.equals(newRequest.ifNoneMatch));
    }

    String withIfNoneMatch(
        String ifNoneMatch)
    {
        return Objects.equals(request.ifNoneMatch, ifNoneMatch) ? ifNoneMatch : null;
    }

    void doRetryRequestImmediatelyIfPending(
        long traceId)
    {
        if (retryRequest != null)
        {
            retryRequest.cancel(true);
            doRetryRequest(traceId);
        }
    }

    private void doRequestAttempt(
        long traceId)
    {
        final ArrayFW<HttpHeaderFW> headers = getRequestHeaders();
        final int initialState = 0;

        attempts++;

        state = HttpCacheRequestState.openingInitial(initialState);
        initialId = factory.supplyInitialId.applyAsLong(routeId);
        initial = factory.router.supplyReceiver(initialId);
        replyId = factory.supplyReplyId.applyAsLong(initialId);

        factory.router.setThrottle(initialId, this::onRequestMessage);
        final long authorization = 0; // TODO: request.authorization
        factory.writer.doHttpRequest(initial, routeId, initialId, traceId, authorization,
                                     mutateRequestHeaders(headers));
        factory.correlations.put(replyId, this::newResponse);
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
                if (!AUTHORIZATION.equals(name) &&
                    !IF_NONE_MATCH.equals(name))
                {
                    builder.item(item -> item.name(name).value(value));
                }
            });

            final String authorizationHeader = requestGroup.authorizationHeader();
            if (authorizationHeader != null)
            {
                builder.item(item -> item.name(AUTHORIZATION).value(authorizationHeader));
            }

            final String ifNoneMatchHeader = requestGroup.ifNoneMatchHeader();
            if (ifNoneMatchHeader != null)
            {
                builder.item(item -> item.name(IF_NONE_MATCH).value(ifNoneMatchHeader));
            }
        };
    }

    private void onNotifyMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onNotifySignal(signal);
            break;
        default:
            break;
        }
    }

    private void onNotifySignal(
        SignalFW signal)
    {
        final long traceId = signal.traceId();
        final int signalId = signal.signalId();

        if (signalId == GROUP_REQUEST_RETRY_SIGNAL)
        {
            doRetryRequest(traceId);
        }
    }

    private void onRequestMessage(
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
        default:
            break;
        }
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        final long traceId = window.traceId();
        state = HttpCacheRequestState.openInitial(state);
        factory.writer.doHttpEnd(initial, routeId, initialId, traceId);
        state = HttpCacheRequestState.closedInitial(state);

        flushResetIfNecessary(traceId);
    }

    private void onRequestReset(
        ResetFW reset)
    {
        final long traceId = reset.traceId();
        factory.correlations.remove(replyId);
        cleanupRequestIfNecessary();

        requestGroup.onGroupRequestReset(request, traceId);
    }

    private void doRetryRequestAfter(
        long retryAfter)
    {
        if (retryAfter <= 0L)
        {
            final long newTraceId = factory.supplyTraceId.getAsLong();
            doRetryRequest(newTraceId);
        }
        else
        {
            retryRequest = factory.executor.schedule(retryAfter,
                                                     SECONDS,
                                                     routeId,
                                                     notifyId,
                                                     GROUP_REQUEST_RETRY_SIGNAL);
            factory.router.setThrottle(notifyId, this::onNotifyMessage);
        }
    }

    private void doRetryRequest(
        long traceId)
    {
        factory.counters.requestsRetry.getAsLong();
        doRequestAttempt(traceId);
    }

    private ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        assert headersSlot != NO_SLOT;
        final MutableDirectBuffer buffer = factory.headersPool.buffer(headersSlot);
        return factory.httpHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    private void cleanupRequestIfNecessary()
    {
        factory.correlations.remove(replyId);
        factory.router.clearThrottle(replyId);
        releaseRequestSlotIfNecessary();

        if (retryRequest != null)
        {
            retryRequest.cancel(true);
            retryRequest = null;
        }
    }

    private void releaseRequestSlotIfNecessary()
    {
        if (headersSlot != NO_SLOT)
        {
            factory.headersPool.release(headersSlot);
            headersSlot = NO_SLOT;
        }
    }

    private MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        final ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        final boolean retry = HttpHeadersUtil.retry(responseHeaders);
        final int requestHash = requestGroup.requestHash();
        final String ifNoneMatch = requestGroup.ifNoneMatchHeader();

        MessageConsumer newStream = null;

        if ((retry && attempts <= 3) ||
            (factory.defaultCache.checkToRetry(getRequestHeaders(),
                                               responseHeaders,
                                               ifNoneMatch,
                                               requestHash)))
        {
            if (requestGroup.hasQueuedRequests() || requestGroup.hasAttachedResponses())
            {
                final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                    new HttpCacheProxyRetryResponse(factory,
                                                    requestHash,
                                                    initial,
                                                    routeId,
                                                    initialId,
                                                    this::doRetryRequestAfter);
                newStream = cacheProxyRetryResponse::onResponseMessage;
                resetHandler = cacheProxyRetryResponse::doResponseReset;
            }
            else
            {
                cleanupRequestIfNecessary();
                requestGroup.onGroupRequestEnd(request);
                state = HttpCacheRequestState.closedReply(state);
            }
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final ArrayFW<HttpHeaderFW> requestHeaders = getRequestHeaders();
            final short authScope = authorizationScope(request.authorization);
            final String requestURL = getRequestURL(requestHeaders);
            final DefaultCacheEntry cacheEntry = factory.defaultCache.supply(requestHash, authScope, requestURL);

            final boolean stored = cacheEntry.storeRequestHeaders(requestHeaders);
            assert stored;

            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    request,
                                                    initial,
                                                    routeId,
                                                    replyId,
                                                    cacheEntry,
                                                    this::doRetryRequestAfter,
                                                    this::cleanupRequestIfNecessary);

            newStream = cacheableResponse::onResponseMessage;
            resetHandler = cacheableResponse::doResponseReset;
        }
        else
        {
            final HttpCacheProxyRelayedResponse relayedResponse = request.newRelayedResponse(initial, routeId, replyId);
            newStream = relayedResponse::onResponseMessage;
            resetHandler = relayedResponse::doResponseReset;
            cleanupRequestIfNecessary();
            requestGroup.onGroupRequestEnd(request);
        }

        return newStream;
    }

    void doResponseReset(
        long traceId)
    {
        factory.router.clearThrottle(notifyId);
        factory.correlations.remove(replyId);
        resetHandler.accept(traceId);
        cleanupRequestIfNecessary();
        state = HttpCacheRequestState.closingReply(state);
        flushResetIfNecessary(traceId);
    }

    private void flushResetIfNecessary(
        long traceId)
    {
        if (HttpCacheRequestState.initialClosed(state) &&
            HttpCacheRequestState.replyClosing(state))
        {
            factory.writer.doReset(initial, routeId, replyId, traceId);
            state = HttpCacheRequestState.closedReply(state);
        }
    }
}
