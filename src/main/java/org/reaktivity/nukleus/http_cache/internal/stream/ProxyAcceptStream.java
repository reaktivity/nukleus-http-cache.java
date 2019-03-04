/**
 * Copyright 2016-2018 The Reaktivity Project
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
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.InitialRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.PreferWaitIfNoneMatchRequest;
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

final class ProxyAcceptStream
{
    private final ProxyStreamFactory streamFactory;
    private final long acceptRouteId;
    private final long acceptStreamId;
    private final MessageConsumer acceptReply;

    private long acceptReplyId;
    private long acceptCorrelationId;

    private MessageConsumer connect;
    private long connectRouteId;
    private long connectInitialId;

    private MessageConsumer streamState;

    private int requestSlot = NO_SLOT;
    private Request request;
    private int requestURLHash;

    ProxyAcceptStream(
        ProxyStreamFactory streamFactory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long connectRouteId)
    {
        this.streamFactory = streamFactory;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptStreamId = acceptStreamId;
        this.connectRouteId = connectRouteId;
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
        final long acceptInitialId = begin.streamId();
        final long authorization = begin.authorization();
        final short authorizationScope = authorizationScope(authorization);

        this.connectInitialId = streamFactory.supplyInitialId.applyAsLong(connectRouteId);
        this.connect = streamFactory.router.supplyReceiver(connectInitialId);

        this.acceptReplyId = streamFactory.supplyReplyId.applyAsLong(acceptInitialId);
        this.acceptCorrelationId = begin.correlationId();

        final OctetsFW extension = streamFactory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();
        final boolean authorizationHeader = requestHeaders.anyMatch(HAS_AUTHORIZATION);

        // Should already be canonicalized in http / http2 nuklei
        final String requestURL = getRequestURL(requestHeaders);

        this.requestURLHash = 31 * authorizationScope + requestURL.hashCode();

        // count all requests
        streamFactory.counters.requests.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [received request]\n",
                    currentTimeMillis(), acceptCorrelationId, getRequestURL(httpBeginFW.headers()));
        }

        if (isPreferIfNoneMatch(requestHeaders))
        {
            streamFactory.counters.requestsPreferWait.getAsLong();
            handlePreferWaitIfNoneMatchRequest(
                    authorizationHeader,
                    authorization,
                    authorizationScope,
                    requestHeaders);
        }
        else if (canBeServedByCache(requestHeaders))
        {
            streamFactory.counters.requestsCacheable.getAsLong();
            handleCacheableRequest(requestHeaders, requestURL, authorizationHeader, authorization, authorizationScope);
        }
        else
        {
            proxyRequest(requestHeaders);
        }
    }

    private short authorizationScope(
        long authorization)
    {
        return (short) (authorization >>> 48);
    }

    private void handlePreferWaitIfNoneMatchRequest(
        boolean authorizationHeader,
        long authorization,
        short authScope,
        ListFW<HttpHeaderFW> requestHeaders)
    {
        String etag = requestHeaders.matchFirst(h -> "etag".equals(h.name().asString())).value().asString();
        final PreferWaitIfNoneMatchRequest preferWaitRequest = new PreferWaitIfNoneMatchRequest(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            acceptCorrelationId,
            streamFactory.router,
            requestURLHash,
            authorizationHeader,
            authorization,
            authScope,
            etag);

        this.request = preferWaitRequest;

        streamFactory.cache.handlePreferWaitIfNoneMatchRequest(
                requestURLHash,
                preferWaitRequest,
                requestHeaders,
                authScope);
        this.streamState = this::onStreamMessageWhenIgnoring;
    }

    private void handleCacheableRequest(
        final ListFW<HttpHeaderFW> requestHeaders,
        final String requestURL,
        boolean authorizationHeader,
        long authorization,
        short authScope)
    {
        boolean stored = storeRequest(requestHeaders, streamFactory.requestBufferPool);
        if (!stored)
        {
            send503RetryAfter();
            return;
        }
        String etag = requestHeaders.matchFirst(h -> "etag".equals(h.name().asString())).value().asString();
        InitialRequest cacheableRequest;
        this.request = cacheableRequest = new InitialRequest(
                streamFactory.cache,
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                acceptCorrelationId,
                connectRouteId,
                streamFactory.supplyCorrelationId,
                streamFactory.supplyInitialId,
                streamFactory.router::supplyReceiver,
                requestURLHash,
                streamFactory.requestBufferPool,
                requestSlot,
                streamFactory.router,
                authorizationHeader,
                authorization,
                authScope,
                etag);

        if (streamFactory.cache.handleInitialRequest(requestURLHash, requestHeaders, authScope, cacheableRequest))
        {
            this.request.purge();
        }
        else if (streamFactory.cache.hasPendingInitialRequests(requestURLHash))
        {
            streamFactory.cache.addPendingRequest(cacheableRequest);
        }
        else if (requestHeaders.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED))
        {
            // TODO move this logic and edge case inside of cache
            send504();
        }
        else
        {
            long connectCorrelationId = streamFactory.supplyCorrelationId.getAsLong();

            if (DEBUG)
            {
                System.out.printf("[%016x] CONNECT %016x %s [sent initial request]\n",
                        currentTimeMillis(), connectCorrelationId, getRequestURL(requestHeaders));
            }

            sendBeginToConnect(requestHeaders, connectCorrelationId);
            streamFactory.writer.doHttpEnd(connect, connectRouteId, connectInitialId,
                    streamFactory.supplyTrace.getAsLong());
            streamFactory.cache.createPendingInitialRequests(cacheableRequest);
        }

        this.streamState = this::onStreamMessageWhenIgnoring;
    }

    private void proxyRequest(
        final ListFW<HttpHeaderFW> requestHeaders)
    {
        this.request = new ProxyRequest(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                acceptCorrelationId,
                streamFactory.router);

        long connectCorrelationId = streamFactory.supplyCorrelationId.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent proxy request]\n",
                    currentTimeMillis(), connectCorrelationId, getRequestURL(requestHeaders));
        }

        sendBeginToConnect(requestHeaders, connectCorrelationId);

        this.streamState = this::onStreamMessageWhenProxying;
    }

    private void sendBeginToConnect(
        final ListFW<HttpHeaderFW> requestHeaders,
        long connectCorrelationId)
    {
        streamFactory.correlations.put(connectCorrelationId, request);

        streamFactory.writer.doHttpRequest(connect, connectRouteId, connectInitialId, connectCorrelationId,
                builder -> requestHeaders.forEach(
                        h ->  builder.item(item -> item.name(h.name()).value(h.value()))
            )
        );

        streamFactory.router.setThrottle(connectInitialId, this::onThrottleMessage);
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

    private void send504()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptCorrelationId, "504");
        }

        streamFactory.writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, acceptCorrelationId, e ->
                e.item(h -> h.representation((byte) 0)
                        .name(STATUS)
                        .value("504")));
        streamFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId,
                streamFactory.supplyTrace.getAsLong());
        request.purge();

        // count all responses
        streamFactory.counters.responses.getAsLong();
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptCorrelationId, "503");
        }

        streamFactory.writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, acceptCorrelationId, e ->
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
            default:
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

        streamFactory.writer.doHttpData(connect, connectRouteId, connectInitialId, groupId, padding,
                                        payload.buffer(), payload.offset(), payload.sizeof());
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
}
