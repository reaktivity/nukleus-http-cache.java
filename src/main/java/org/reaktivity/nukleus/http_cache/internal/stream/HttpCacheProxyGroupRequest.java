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

import static java.lang.System.currentTimeMillis;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_IN_FLIGHT_ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_RETRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
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
    private static final String16FW HEADER_VALUE_STATUS_503 = new String16FW("503");
    private final MutableDirectBuffer writeBuffer;

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final long acceptInitialId;
    private final long acceptReplyId;
    private final long acceptRouteId;
    private final MessageConsumer acceptReply;

    private long initialId;
    private long replyId;

    private MessageConsumer connectInitial;
    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private final MutableInteger requestSlot;
    private String ifNoneMatch;
    private Future<?> retryRequest;
    private int attempts;

    HttpCacheProxyGroupRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        long acceptReplyId,
        long acceptRouteId)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.acceptReplyId = acceptReplyId;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptReplyId | 0x1;
        this.acceptReply = factory.router.supplyReceiver(acceptReplyId);
        this.requestSlot =  new MutableInteger(NO_SLOT);
        this.writeBuffer = new UnsafeBuffer(allocateDirect(factory.writer.writerCapacity()).order(nativeOrder()));
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        MessageConsumer newStream = null;
        ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);

        if ((retry && attempts < 3) ||
            (factory.defaultCache.checkToRetry(getRequestHeaders(),
                                               responseHeaders,
                                               ifNoneMatch,
                                               requestGroup.getRequestHash())))
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                requestGroup.getRequestHash(),
                                                connectReply,
                                                connectRouteId,
                                                connectReplyId,
                                                this::scheduleRequest);
            newStream = cacheProxyRetryResponse::onResponseMessage;
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestGroup,
                                                    requestGroup.getRequestHash(),
                                                    requestSlot,
                                                    connectReply,
                                                    connectReplyId,
                                                    connectRouteId,
                                                    ifNoneMatch,
                                                    this::scheduleRequest);
            newStream = cacheableResponse::onResponseMessage;
        }
        else
        {
            Function<HttpBeginExFW, MessageConsumer> responseFactory = factory.correlations.remove(replyId);
            if (responseFactory != null)
            {
                MessageConsumer newResponse = responseFactory.apply(beginEx);
                newStream = (t, b, i, l) ->
                {
                    writeBuffer.putBytes(0, b, i, l);
                    switch (t)
                    {
                    case BeginFW.TYPE_ID:
                    case DataFW.TYPE_ID:
                    case EndFW.TYPE_ID:
                    case AbortFW.TYPE_ID:
                        writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, replyId);
                        newResponse.accept(t, writeBuffer, 0, l);
                        break;
                    case ResetFW.TYPE_ID:
                    case WindowFW.TYPE_ID:
                    case SignalFW.TYPE_ID:
                        writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, initialId);
                        newResponse.accept(t, writeBuffer, 0, l);
                        break;
                    default:
                        break;
                    }

                };
                factory.router.setThrottle(replyId, this::onResponseMessage);
            }
        }

        return newStream;
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
        connectRouteId = begin.routeId();
        initialId = begin.streamId();
        replyId = factory.supplyReplyId.applyAsLong(initialId);
        factory.router.setThrottle(replyId, this::onResponseMessage);
        factory.router.setThrottle(initialId, this::onResponseMessage);
        // count all requests

        boolean stored = storeRequest(requestHeaders);
        if (!stored)
        {
            //TODO find different way to handle it.
            send503RetryAfter();
            return;
        }
        HttpHeaderFW ifNoneMatchHeader = requestHeaders.matchFirst(h -> h.name().asString().equals(IF_NONE_MATCH));
        if (ifNoneMatchHeader != null)
        {
            ifNoneMatch = ifNoneMatchHeader.value().asString();
        }
        doHttpBegin(requestHeaders);
    }

    private void onData(
        final DataFW data)
    {
        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                data.trace(),
                                data.reserved(),
                                0,
                                data.groupId());
    }

    private void onEnd(
        final EndFW end)
    {
        //NOOP
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
        cleanupRequestIfNecessary();
        //TODO: signal accept side to clean up
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        factory.writer.doHttpEnd(connectInitial,
                                 connectRouteId,
                                 connectInitialId,
                                 window.trace());
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        //TODO: Signal accept side to reset request and clean up
        cleanupRequestIfNecessary();
    }

    private void onRequestSignal(
        SignalFW signal)
    {
        final int signalId = (int) signal.signalId();

        switch (signalId)
        {
        case REQUEST_RETRY_SIGNAL:
            retryCacheableRequest();
            break;
        case REQUEST_IN_FLIGHT_ABORT_SIGNAL:
            onResponseSignalInFlightRequestAborted(signal);
            break;
        default:
            break;
        }
    }

    private void onWindow(
        final WindowFW window)
    {
        factory.writer.doWindow(connectReply,
                                connectRouteId,
                                connectReplyId,
                                window.trace(),
                                window.credit(),
                                window.padding(),
                                window.groupId());
    }

    private void onReset(
        final ResetFW reset)
    {
        factory.writer.doReset(connectReply,
                               connectRouteId,
                               connectReplyId,
                               reset.trace());
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
                                                     connectRouteId,
                                                     replyId,
                                                     REQUEST_RETRY_SIGNAL);
        }
        return true;
    }

    private void retryCacheableRequest()
    {
        incAttempts();

        connectInitialId = factory.supplyInitialId.applyAsLong(connectRouteId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectInitial = factory.router.supplyReceiver(connectInitialId);

        factory.correlations.put(connectReplyId, this::newResponse);
        ArrayFW<HttpHeaderFW> requestHeaders = getRequestHeaders();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTrace.getAsLong(),
                                     mutateRequestHeaders(requestHeaders));
        factory.counters.requestsRetry.getAsLong();
        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
    }

    private boolean storeRequest(
        final ArrayFW<HttpHeaderFW> headers)
    {
        assert requestSlot.value == NO_SLOT;
        int newRequestSlot = factory.requestBufferPool.acquire(acceptInitialId);
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

    private void onResponseSignalInFlightRequestAborted(
        SignalFW signal)
    {
        if (retryRequest != null)
        {
            retryRequest.cancel(true);
        }
        factory.writer.doAbort(connectInitial,
                               connectRouteId,
                               connectInitialId,
                               factory.supplyTrace.getAsLong());
        factory.writer.doReset(connectReply,
                               connectRouteId,
                               connectReplyId,
                               factory.supplyTrace.getAsLong());
    }

    private void doHttpBegin(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        connectInitialId = factory.supplyInitialId.applyAsLong(connectRouteId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectReply = factory.router.supplyReceiver(connectReplyId);
        connectInitial = factory.router.supplyReceiver(connectInitialId);
        factory.correlations.put(connectReplyId, this::newResponse);

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTrace.getAsLong(),
                                     mutateRequestHeaders(requestHeaders));
        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
    }

    private void send503RetryAfter()
    {

        factory.writer.doHttpResponse(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTrace.getAsLong(),
            e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                  .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTrace.getAsLong());

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();
    }

    private void cleanupRequestIfNecessary()
    {
        requestGroup.dequeue(ifNoneMatch, acceptReplyId);
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }
    }
}
