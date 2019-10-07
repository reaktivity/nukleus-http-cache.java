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

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_ABORTED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_NOT_MODIFIED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_IN_FLIGHT_ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_RETRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil;
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
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyConnectRequest
{

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private MessageConsumer connectInitial;
    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private final MutableInteger requestSlot;
    private int requestHash;
    private boolean isRequestPurged;
    private String ifNoneMatch;
    private Future<?> preferWaitExpired;
    private Future<?> retryRequest;
    private int attempts;

    private int acceptReplyBudget;
    private long groupId;
    private int padding;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private boolean requestQueued;
    private boolean requestExpired;

    HttpCacheProxyConnectRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
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
        this.requestGroup = requestGroup;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectInitial = connectInitial;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
        this.requestSlot =  new MutableInteger(NO_SLOT);
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        assert !isRequestPurged;

        MessageConsumer newStream;
        ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);

        if ((retry && attempts < 3) ||
            (factory.defaultCache.checkToRetry(getRequestHeaders(),
                                               responseHeaders,
                                               ifNoneMatch,
                                               requestHash)) &&
            !requestGroup.isInFlightRequestAborted())
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                requestHash,
                                                connectReply,
                                                connectRouteId,
                                                connectReplyId,
                                                this::scheduleRequest);
            newStream = cacheProxyRetryResponse::onResponseMessage;

        }
        else if (factory.defaultCache.matchCacheableResponse(requestHash,
                                                             getHeader(responseHeaders, ETAG),
                                                             getRequestHeaders().anyMatch(HAS_IF_NONE_MATCH)))
        {
            final HttpCacheProxyNotModifiedResponse notModifiedResponse =
                new HttpCacheProxyNotModifiedResponse(factory,
                                                      requestHash,
                                                      getHeader(getRequestHeaders(), PREFER),
                                                      acceptReply,
                                                      acceptRouteId,
                                                      acceptReplyId,
                                                      connectReply,
                                                      connectReplyId,
                                                      connectRouteId);
            factory.router.setThrottle(acceptReplyId, notModifiedResponse::onResponseMessage);
            newStream = notModifiedResponse::onResponseMessage;
            cleanupRequestIfNecessary();
            requestGroup.onNonCacheableResponse(ifNoneMatch, acceptReplyId);
            resetRequestTimeoutIfNecessary();
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestGroup,
                                                    requestHash,
                                                    requestSlot,
                                                    acceptReply,
                                                    acceptRouteId,
                                                    acceptReplyId,
                                                    connectReply,
                                                    connectReplyId,
                                                    connectRouteId,
                                                    ifNoneMatch,
                                                    this::scheduleRequest);
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
            requestGroup.onNonCacheableResponse(ifNoneMatch, acceptReplyId);
            cleanupRequestIfNecessary();
            resetRequestTimeoutIfNecessary();
        }

        return newStream;
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
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onResponseSignal(signal);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onResponseReset(reset);
            break;
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
            retryRequest = this.factory.executor.schedule(retryAfter,
                                                          MILLISECONDS,
                                                          this.acceptRouteId,
                                                          this.acceptReplyId,
                                                          REQUEST_RETRY_SIGNAL);
        }
        return true;
    }

    private void retryCacheableRequest()
    {
        if (isRequestPurged)
        {
            return;
        }

        incAttempts();

        connectInitialId = this.factory.supplyInitialId.applyAsLong(connectRouteId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectInitial = this.factory.router.supplyReceiver(connectInitialId);

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
        requestQueued = true;
    }


    private ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot.value);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    private void purge()
    {
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }
        isRequestPurged = true;
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
            if (ifNoneMatch != null &&
                !requestGroup.isInFlightRequestAborted())
            {
                builder.item(item -> item.name(IF_NONE_MATCH).value(ifNoneMatch));
            }
        };
    }
}
