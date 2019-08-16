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
import org.reaktivity.nukleus.http_cache.internal.proxy.request.DefaultRequest;
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
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.satisfiedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

public final class HttpCacheProxyCacheableRequest extends HttpCacheProxyRequest
{
    private int requestSlot = NO_SLOT;
    private DefaultRequest request;
    private int requestHash;
    private Future<?> preferWaitExpired;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory streamFactory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long acceptReplyId,
        MessageConsumer connectInitial,
        MessageConsumer connectReply,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {
        super(streamFactory,
              acceptReply,
              acceptRouteId,
              acceptStreamId,
              acceptReplyId,
              connectInitial,
              connectReply,
              connectInitialId,
              connectReplyId,
              connectRouteId);

    }

    public int attempts()
    {
        return request.attempts();
    }

    public ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> requestHeaders)
    {
        return request.getRequestHeaders(requestHeaders);
    }

    public String etag()
    {
        return request.etag();
    }

    public int requestHash()
    {
        return request.requestHash();
    }

    public boolean isRequestPurged()
    {
        return request.isRequestPurged();
    }

    public void incAttempts()
    {
        request.incAttempts();
    }

    public void purge()
    {
        request.purge();
    }

    public LongUnaryOperator supplyInitialId()
    {
        return request.supplyInitialId();
    }

    public LongFunction<MessageConsumer> supplyReceiver()
    {
        return request.supplyReceiver();
    }

    public LongUnaryOperator supplyReplyId()
    {
        return request.supplyReplyId();
    }

    public MessageConsumer getSignaler()
    {
        return request.getSignaler();
    }

    public HttpCacheProxyResponse newResponse(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        if (responseHeaders == null || isCacheableResponse(responseHeaders))
        {
            return new HttpCacheProxyCacheableResponse(streamFactory,
                                                      this,
                                                      connectReply,
                                                      connectRouteId,
                                                      connectReplyId,
                                                      acceptStreamId);
        }
        else
        {
            return new HttpCacheProxyNonCacheableResponse(streamFactory,
                                                          this,
                                                          connectReply,
                                                          connectRouteId,
                                                          connectReplyId,
                                                          acceptStreamId);
        }
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
                streamFactory.writer.doReset(acceptReply,
                                             acceptRouteId,
                                             acceptStreamId,
                                             streamFactory.supplyTrace.getAsLong());
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
                final BeginFW begin = streamFactory.beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = streamFactory.abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = streamFactory.resetRO.wrap(buffer, index, index + length);
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

        handleRequest(requestHeaders, authorizationHeader, authorization);
    }

    private void onData(
        final DataFW data)
    {
        streamFactory.writer.doWindow(acceptReply,
                                      acceptRouteId,
                                      acceptStreamId,
                                      data.trace(),
                                      data.sizeof(),
                                      data.padding(),
                                      data.groupId());
    }

    private void onEnd(
        final EndFW end)
    {
        final long traceId = end.trace();
        streamFactory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, traceId);
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        streamFactory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
        streamFactory.cleanupCorrelationIfNecessary(connectReplyId, acceptStreamId);
        request.purge();
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

    private void onReset(
        final ResetFW reset)
    {
        final long traceId = reset.trace();
        streamFactory.writer.doReset(acceptReply, acceptRouteId, acceptStreamId, traceId);

        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
        }
        Request request = this.streamFactory.requestCorrelations.remove(connectReplyId);
        if (request != null && request.getType() == Request.Type.DEFAULT_REQUEST)
        {
            this.streamFactory.defaultCache.removePendingInitialRequest(this);
        }
        streamFactory.cleanupCorrelationIfNecessary(connectReplyId, acceptStreamId);
    }

    private void handleRequest(
        final ListFW<HttpHeaderFW> requestHeaders,
        boolean authorizationHeader,
        long authorization)
    {
        if (satisfiedByCache(requestHeaders))
        {
            streamFactory.counters.requestsCacheable.getAsLong();
        }

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

        this.request = new DefaultRequest(acceptReply,
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

        if (satisfiedByCache(requestHeaders) &&
            streamFactory.defaultCache.handleCacheableRequest(streamFactory, requestHeaders, authScope, this))
        {
            //NOOP
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
            streamFactory.defaultCache.createPendingInitialRequests(this);
            schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
        }

    }

    private void schedulePreferWaitIfNoneMatchIfNecessary(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = this.streamFactory.executor.schedule(preferWait,
                                                                         SECONDS,
                                                                         acceptRouteId,
                                                                         this.acceptReplyId,
                                                                         REQUEST_EXPIRED_SIGNAL);
                streamFactory.expiryRequestsCorrelations.put(this.acceptReplyId, preferWaitExpired);
            }
        }
    }


    private void sendBeginToConnect(
        final ListFW<HttpHeaderFW> requestHeaders,
        long connectCorrelationId)
    {
        streamFactory.requestCorrelations.put(connectCorrelationId, request);

        streamFactory.writer.doHttpRequest(
            connectInitial,
            connectRouteId,
            connectInitialId,
            streamFactory.supplyTrace.getAsLong(),
            builder -> requestHeaders.forEach(
                h ->  builder.item(item -> item.name(h.name()).value(h.value()))
                                             ));

        streamFactory.router.setThrottle(connectInitialId, this::onRequestMessage);
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

        streamFactory.writer.doHttpResponse(acceptReply,
                                            acceptRouteId,
                                            acceptReplyId,
                                            streamFactory.supplyTrace.getAsLong(), e ->
                                            e.item(h -> h.name(STATUS).value("503"))
                                             .item(h -> h.name("retry-after").value("0")));

        streamFactory.writer.doHttpEnd(acceptReply,
                                       acceptRouteId,
                                       acceptReplyId,
                                       streamFactory.supplyTrace.getAsLong());

        // count all responses
        streamFactory.counters.responses.getAsLong();

        // count retry responses
        streamFactory.counters.responsesRetry.getAsLong();
    }

    private static short authorizationScope(
        long authorization)
    {
        return (short) (authorization >>> 48);
    }
}
