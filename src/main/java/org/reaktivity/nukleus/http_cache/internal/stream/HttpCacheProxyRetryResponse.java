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
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpEndExFW;

import java.time.Instant;
import java.util.concurrent.Future;

final class HttpCacheProxyRetryResponse
{
    private final HttpCacheProxyFactory streamFactory;

    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyStreamId;
    private int connectReplyBudget;

    private long acceptInitialId;
    private int acceptReplyBudget;

    private long groupId;
    private int padding;

    private final int initialWindow;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private long traceId;
    private boolean isResponseBuffering;

    HttpCacheProxyRetryResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        long acceptInitialId)
    {
        this.streamFactory = httpCacheProxyFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyStreamId = connectReplyId;
        this.acceptInitialId = acceptInitialId;

        this.initialWindow = this.streamFactory.responseBufferPool.slotCapacity();
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, acceptReplyBudget=%016x, " +
                             "connectReplyBudget=%d, padding=%d]", getClass().getSimpleName(),
                             connectRouteId, connectReplyStreamId, acceptReplyBudget, connectReplyBudget, padding);
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        traceId = begin.trace();

        final OctetsFW extension = streamFactory.beginRO.extension();

        if (extension.sizeof() > 0)
        {
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);

            if (DEBUG)
            {
                System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                                  getHeader(httpBeginFW.headers(), ":status"));
            }

            final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
            handleInitialRequest(responseHeaders);
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, traceId);
        }
    }

    ///////////// INITIAL_REQUEST REQUEST
    private void handleInitialRequest(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        if (request.isRequestPurged() || performRetryRequestIfNecessary(responseHeaders))
        {
            return;
        }

        handleCacheableResponse(responseHeaders);
    }

    private boolean performRetryRequestIfNecessary(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        String status = getHeader(responseHeaders, STATUS);
        long retryAfter = HttpHeadersUtil.retryAfter(responseHeaders);
        assert status != null;

        if ((retry
             && request.attempts() < 3)
            || !this.streamFactory.defaultCache.isUpdatedByResponseHeadersToRetry(request, responseHeaders))
        {

            scheduleRequest(retryAfter);
            return true;
        }
        return false;
    }




    private void handleCacheableResponse(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        DefaultCacheEntry cacheEntry = this.streamFactory.defaultCache.supply(request.requestHash());
        String newEtag = getHeader(responseHeaders, ETAG);

        if (newEtag == null)
        {
            isResponseBuffering = true;
        }

        //Initial cache entry
        if(cacheEntry.etag() == null && cacheEntry.requestHeadersSize() == 0)
        {
            if (!cacheEntry.storeRequestHeaders(request.getRequestHeaders(streamFactory.requestHeadersRO))
                || !cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //TODO: Better handle if there is no slot available, For example, release response payload
                // which requests are in flight
                request.purge();
            }
            if(!isResponseBuffering)
            {
                this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestHash());
            }
            this.streamState = this::handleCacheableRequestResponse;

        }
        else
        {
            if (cacheEntry.etag() != null
                && cacheEntry.etag().equals(newEtag)
                && cacheEntry.recentAuthorizationHeader() != null)
            {
                this.streamFactory.defaultCache.send304(cacheEntry, request);
                this.streamState = this::handleNoopStream;
            }
            else
            {
                cacheEntry.evictResponse();
                if (!cacheEntry.storeResponseHeaders(responseHeaders))
                {
                    //TODO: Better handle if there is no slot available, For example, release response payload
                    // which requests are in flight
                    request.purge();
                }
                this.streamState = this::handleCacheableRequestResponse;
                if(!isResponseBuffering)
                {
                    this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestHash());
                }
            }
        }

        sendWindow(initialWindow, traceId);
    }


    private void handle503Retry(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        //NOOP
    }

    private void handleNoopStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case BeginFW.TYPE_ID:
                final BeginFW begin = this.streamFactory.beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                sendWindow(data.length() + data.padding(), data.trace());
                break;
            case EndFW.TYPE_ID:
                break;
            case AbortFW.TYPE_ID:
            default:
                //TODO: Figure out what to do with abort on response.
                streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
                break;
        }
    }


    private void sendWindow(
        int credit,
        long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                                          connectReplyStreamId, traceId, credit, padding, groupId);
        }
    }

    public void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
            case WindowFW.TYPE_ID:
                final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
                groupId = window.groupId();
                padding = window.padding();
                long streamId = window.streamId();
                int credit = window.credit();
                acceptReplyBudget += credit;
                this.streamFactory.budgetManager.window(BudgetManager.StreamKind.CACHE, groupId, streamId, credit,
                                                        this::writePayload, window.trace());
                sendEndIfNecessary(window.trace());
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = streamFactory.signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            case ResetFW.TYPE_ID:
            default:
                this.streamFactory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                        groupId,
                                                        request.acceptReplyId,
                                                        this.streamFactory.supplyTrace.getAsLong());
                streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
                this.streamFactory.defaultCache.removePendingInitialRequest(request);
                request.purge();
                streamFactory.writer.doReset(request.acceptReply,
                                             request.acceptRouteId,
                                             acceptInitialId,
                                             streamFactory.supplyTrace.getAsLong());
                break;
        }
    }

    private void onSignal(
        SignalFW signal)
    {
        final long signalId = signal.signalId();

        if (signalId == CACHE_ENTRY_UPDATED_SIGNAL || signalId == CACHE_ENTRY_SIGNAL)
        {
            handleCacheUpdateSignal(signal);
        }
        else if (signalId == REQUEST_EXPIRED_SIGNAL)
        {
            this.streamFactory.defaultCache.send304ToPendingInitialRequests(request.requestHash());
            streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
        }
        else if (signalId == ABORT_SIGNAL)
        {
            if (this.payloadWritten >= 0)
            {
                streamFactory.writer.doAbort(request.acceptReply,
                                             request.acceptRouteId,
                                             request.acceptReplyId,
                                             signal.trace());
                streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
            }
            else
            {
                send503RetryAfter();
            }


        }
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(),
                              request.acceptReplyId, "503");
        }

        streamFactory.writer.doHttpResponse(request.acceptReply,
                                            request.acceptRouteId,
                                            request.acceptReplyId,
                                            streamFactory.supplyTrace.getAsLong(), e ->
                                                e.item(h -> h.name(STATUS).value(SERVICE_UNAVAILABLE_503))
                                                 .item(h -> h.name("retry-after").value("0")));
        streamFactory.writer.doHttpEnd(request.acceptReply,
                                       request.acceptRouteId,
                                       request.acceptReplyId,
                                       streamFactory.supplyTrace.getAsLong());

        // count all responses
        streamFactory.counters.responses.getAsLong();

        // count retry responses
        streamFactory.counters.responsesRetry.getAsLong();
    }

}