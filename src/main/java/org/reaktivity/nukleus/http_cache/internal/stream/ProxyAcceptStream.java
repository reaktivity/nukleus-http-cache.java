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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.satisfiedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.DefaultRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.ProxyRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
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

import java.util.concurrent.Future;

final class ProxyAcceptStream
{
    private final ProxyStreamFactory streamFactory;
    private final long acceptRouteId;
    private final long acceptStreamId;
    private final MessageConsumer acceptReply;

    private long acceptReplyId;

    private MessageConsumer connect;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private MessageConsumer streamState;

    private int requestSlot = NO_SLOT;
    private Request request;
    private int requestHash;
    private Future<?> preferWaitExpired;

    ProxyAcceptStream(
        ProxyStreamFactory streamFactory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long acceptReplyId,
        MessageConsumer connect,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {
        this.streamFactory = streamFactory;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptStreamId = acceptStreamId;
        this.acceptReplyId = acceptReplyId;
        this.connect = connect;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
        this.streamState = this::beforeBegin;
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = streamFactory.beginRO.wrap(buffer, index, index + length);
            onBegin(begin);
        }
        else
        {
            streamFactory.writer.doReset(acceptReply, acceptRouteId, acceptStreamId,
                    streamFactory.supplyTrace.getAsLong());
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long authorization = begin.authorization();
        final short authorizationScope = authorizationScope(authorization);

        final OctetsFW extension = streamFactory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();
        final boolean authorizationHeader = requestHeaders.anyMatch(HAS_AUTHORIZATION);

        // Should already be canonicalized in http / http2 nuklei
        final String requestURL = getRequestURL(requestHeaders);

        this.requestHash = 31 * authorizationScope + requestURL.hashCode();

        // count all requests
        streamFactory.counters.requests.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [received request]\n",
                    currentTimeMillis(), acceptReplyId, getRequestURL(httpBeginFW.headers()));
        }

        if (canBeServedByCache(requestHeaders))
        {
            if (satisfiedByCache(requestHeaders))
            {
                streamFactory.counters.requestsCacheable.getAsLong();
            }

            handleRequest(requestHeaders, authorizationHeader, authorization);
        }
        else
        {
            proxyRequest(requestHeaders);
        }
    }

    private static short authorizationScope(
        long authorization)
    {
        return (short) (authorization >>> 48);
    }

    private void handleRequest(
        final ListFW<HttpHeaderFW> requestHeaders,
        boolean authorizationHeader,
        long authorization)
    {
        boolean stored = storeRequest(requestHeaders, streamFactory.requestBufferPool);
        if (!stored)
        {
            send503RetryAfter();
            return;
        }

        String etag = null;
        short authScope = authorizationScope(authorization);
        HttpHeaderFW etagHeader = requestHeaders.matchFirst(h -> IF_NONE_MATCH.equals(h.name().asString()));
        if (etagHeader != null)
        {
            etag = etagHeader.value().asString();
        }
        DefaultRequest defaultRequest;
        this.request = defaultRequest = new DefaultRequest(
                acceptReply,
                acceptRouteId,
                acceptStreamId,
                acceptReplyId,
                connectRouteId,
                connectReplyId,
                streamFactory.router,
                streamFactory.router::supplyReceiver,
                requestHash,
                streamFactory.requestBufferPool,
                requestSlot,
                authorizationHeader,
                authorization,
                authScope,
                etag,
                streamFactory.supplyInitialId,
                streamFactory.supplyReplyId,
                false);

        if (satisfiedByCache(requestHeaders)
            && streamFactory.defaultCache.handleCacheableRequest(streamFactory, requestHeaders, authScope, defaultRequest))
        {
            //NOOP
        }
        else if (streamFactory.defaultCache.hasPendingInitialRequests(requestHash))
        {
            streamFactory.defaultCache.addPendingRequest(defaultRequest);
        }
        else if (requestHeaders.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED))
        {
            send504();
        }
        else
        {
            long connectReplyId = streamFactory.supplyReplyId.applyAsLong(connectInitialId);

            if (DEBUG)
            {
                System.out.printf("[%016x] CONNECT %016x %s [sent initial request]\n",
                        currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
            }

            sendBeginToConnect(requestHeaders, connectReplyId);
            streamFactory.writer.doHttpEnd(connect, connectRouteId, connectInitialId,
                    streamFactory.supplyTrace.getAsLong());
            streamFactory.defaultCache.createPendingInitialRequests(defaultRequest);

            scheduleRequestExpiredSignal(requestHeaders);
        }

        this.streamState = this::onStreamMessageWhenIgnoring;
    }

    private void scheduleRequestExpiredSignal(ListFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = this.streamFactory.executor.schedule(preferWait,
                                                                         SECONDS,
                                                                         acceptRouteId,
                                                                         request.acceptReplyStreamId,
                                                                         REQUEST_EXPIRED_SIGNAL);
                streamFactory.expiryRequestsCorrelations.put(request.acceptReplyStreamId, preferWaitExpired);
            }
        }
    }

    private void proxyRequest(
        final ListFW<HttpHeaderFW> requestHeaders)
    {
        this.request = new ProxyRequest(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                streamFactory.router,
                false);

        long connectReplyId = streamFactory.supplyReplyId.applyAsLong(connectInitialId);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent proxy request]\n",
                    currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        sendBeginToConnect(requestHeaders, connectReplyId);

        this.streamState = this::onStreamMessageWhenProxying;
    }

    private void sendBeginToConnect(
        final ListFW<HttpHeaderFW> requestHeaders,
        long connectCorrelationId)
    {
        streamFactory.requestCorrelations.put(connectCorrelationId, request);

        streamFactory.writer.doHttpRequest(
                                        connect,
                                        connectRouteId,
                                        connectInitialId,
                                        streamFactory.supplyTrace.getAsLong(),
                                        builder -> requestHeaders.forEach(
                                            h ->  builder.item(item -> item.name(h.name()).value(h.value()))
         ));

        streamFactory.router.setThrottle(connectInitialId, this::onThrottleMessage);
    }

    private void send504()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId, "504");
        }

        streamFactory.writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, streamFactory.supplyTrace.getAsLong(), e ->
            e.item(h -> h.name(STATUS)
                         .value("504")));
        streamFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId,
                                     streamFactory.supplyTrace.getAsLong());
        request.purge();

        // count all responses
        streamFactory.counters.responses.getAsLong();
    }

    private boolean storeRequest(
        final ListFW<HttpHeaderFW> headers,
        final BufferPool bufferPool)
    {
        this.requestSlot = bufferPool.acquire(acceptStreamId);
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

        streamFactory.writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, streamFactory.supplyTrace.getAsLong(), e ->
                e.item(h -> h.name(STATUS).value("503"))
                 .item(h -> h.name("retry-after").value("0")));
        streamFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId,
                streamFactory.supplyTrace.getAsLong());

        // count all responses
        streamFactory.counters.responses.getAsLong();

        // count retry responses
        streamFactory.counters.responsesRetry.getAsLong();
    }

    private void onStreamMessageWhenIgnoring(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case AbortFW.TYPE_ID:
                cleanupRequestIfNecessary();
                break;
            default:
                break;
        }
    }

    private void onStreamMessageWhenProxying(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
            onDataWhenProxying(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
            onEndWhenProxying(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = streamFactory.abortRO.wrap(buffer, index, index + length);
            onAbortWhenProxying(abort);
            break;
        default:
            streamFactory.writer.doReset(acceptReply, acceptRouteId, acceptStreamId,
                    streamFactory.supplyTrace.getAsLong());
            break;
        }
    }

    private void onDataWhenProxying(
        final DataFW data)
    {
        final long groupId = data.groupId();
        final int padding = data.padding();
        final OctetsFW payload = data.payload();

        streamFactory.writer.doHttpData(connect,
                                        connectRouteId,
                                        connectInitialId,
                                        data.trace(),
                                        groupId,
                                        payload.buffer(),
                                        payload.offset(),
                                        payload.sizeof(),
                                        padding);
    }

    private void onEndWhenProxying(
        final EndFW end)
    {
        final long traceId = end.trace();
        streamFactory.writer.doHttpEnd(connect, connectRouteId, connectInitialId, traceId);
    }

    private void onAbortWhenProxying(
        final AbortFW abort)
    {
        streamFactory.cleanupCorrelationIfNecessary(connectReplyId, acceptStreamId);
        final long traceId = abort.trace();
        streamFactory.writer.doAbort(connect, connectRouteId, connectInitialId, traceId);
        request.purge();
    }

    private void onThrottleMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
            onWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = streamFactory.resetRO.wrap(buffer, index, index + length);
            final long traceId = reset.trace();
            streamFactory.writer.doReset(acceptReply, acceptRouteId, acceptStreamId, traceId);
            this.cleanupRequestIfNecessary();
            break;
        default:
            break;
        }
    }

    private void onWindow(
        final WindowFW window)
    {
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();
        final long traceId = window.trace();
        streamFactory.writer.doWindow(acceptReply, acceptRouteId, acceptStreamId, traceId, credit, padding, groupId);
    }

    private void cleanupRequestIfNecessary()
    {
        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
        }
        Request request = this.streamFactory.requestCorrelations.remove(connectReplyId);
        if (request != null && request.getType() == Request.Type.DEFAULT_REQUEST)
        {
            this.streamFactory.defaultCache.removePendingInitialRequest((DefaultRequest) request);
            streamFactory.cleanupCorrelationIfNecessary(connectReplyId, acceptStreamId);
        }
    }
}
