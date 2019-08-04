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
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
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
import org.reaktivity.nukleus.http_cache.internal.proxy.request.DefaultRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
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

import java.util.concurrent.Future;

final class ProxyConnectReplyStream
{
    private final ProxyStreamFactory streamFactory;

    private MessageConsumer streamState;

    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyStreamId;
    private int connectReplyBudget;

    private Request streamCorrelation;
    private long acceptInitialId;
    private int acceptReplyBudget;

    private long groupId;
    private int padding;

    private final int initialWindow;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private long traceId;
    private String newEtag;

    ProxyConnectReplyStream(
        ProxyStreamFactory proxyStreamFactory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        long acceptInitialId)
    {
        this.streamFactory = proxyStreamFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyStreamId = connectReplyId;
        this.acceptInitialId = acceptInitialId;
        this.streamState = this::beforeBegin;

        this.initialWindow = this.streamFactory.responseBufferPool.slotCapacity();
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, acceptReplyBudget=%016x, " +
                "connectReplyBudget=%d, padding=%d]", getClass().getSimpleName(),
            connectRouteId, connectReplyStreamId, acceptReplyBudget, connectReplyBudget, padding);
    }

    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = this.streamFactory.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                    streamFactory.supplyTrace.getAsLong());
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        traceId = begin.trace();

        this.streamCorrelation = this.streamFactory.requestCorrelations.remove(connectReplyId);
        final OctetsFW extension = streamFactory.beginRO.extension();

        if (streamCorrelation != null && extension.sizeof() > 0)
        {
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);

            if (DEBUG)
            {
                System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                        getHeader(httpBeginFW.headers(), ":status"));
            }

            final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

            switch(streamCorrelation.getType())
            {
                case PROXY:
                    doProxyBegin(responseHeaders);
                    break;
                case DEFAULT_REQUEST:
                    handleInitialRequest(responseHeaders);
                    break;
                default:
                    throw new RuntimeException("Not implemented");
            }
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
        DefaultRequest request = (DefaultRequest)streamCorrelation;
        if (performRetryRequestIfNecessary(request, responseHeaders))
        {
            return;
        }

        if (isCacheableResponse(responseHeaders))
        {
            handleCacheableResponse(responseHeaders);
        }
        else
        {
            streamCorrelation.purge();
            doProxyBegin(responseHeaders);
            this.streamFactory.defaultCache.removePendingInitialRequest(request);
            this.streamFactory.defaultCache.sendPendingInitialRequests(request.requestURLHash());
        }
    }

    private boolean performRetryRequestIfNecessary(
        DefaultRequest request,
        ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        String status = getHeader(responseHeaders, STATUS);
        long retryAfter = HttpHeadersUtil.retryAfter(responseHeaders);
        assert status != null;

        if (retry
            && request.attempts() < 3)
        {
            if (retryAfter <= 0L)
            {
                retryCacheableRequest();
            }
            else
            {
                this.streamFactory.scheduler.accept(retryAfter, this::retryCacheableRequest);
            }
            return true;
        }
        else if (!this.streamFactory.defaultCache.isUpdatedByResponseHeadersToRetry(request, responseHeaders))
        {
            if (retryAfter <= 0L)
            {
                retryCacheableRequest();
            }
            else
            {
                this.streamFactory.scheduler.accept(retryAfter, this::retryCacheableRequest);
            }
            return true;
        }
        return false;
    }

    private void retryCacheableRequest()
    {
        DefaultRequest request = (DefaultRequest) streamCorrelation;
        request.incAttempts();

        long connectInitialId = this.streamFactory.supplyInitialId.applyAsLong(connectRouteId);
        MessageConsumer connectInitial = this.streamFactory.router.supplyReceiver(connectInitialId);
        long connectReplyId = streamFactory.supplyReplyId.applyAsLong(connectInitialId);

        streamFactory.requestCorrelations.put(connectReplyId, request);
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(streamFactory.requestHeadersRO);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                    currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        streamFactory.writer.doHttpRequest(connectInitial, connectRouteId, connectInitialId, traceId, builder ->
        {
            requestHeaders.forEach(
                    h ->  builder.item(item -> item.name(h.name()).value(h.value())));
        });
        streamFactory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, streamFactory.supplyTrace.getAsLong());
        streamFactory.counters.requestsRetry.getAsLong();
        this.streamState = this::handle503Retry;
        this.streamFactory.initializeNewConnectReplyStream(connectInitialId, connectRouteId, acceptInitialId);
    }

    private void handleCacheableResponse(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        DefaultRequest request = (DefaultRequest) streamCorrelation;
        DefaultCacheEntry cacheEntry = this.streamFactory.defaultCache.supply(request.requestURLHash());

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
            this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestURLHash());
            this.streamState = this::handleCacheableRequestResponse;

        }
        else
        {
            newEtag = getHeader(responseHeaders, ETAG);
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
                if(newEtag != null)
                {
                    this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestURLHash());
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

    private void handleCacheableRequestResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        DefaultRequest request = (DefaultRequest) streamCorrelation;
        DefaultCacheEntry cacheEntry = this.streamFactory.defaultCache.get(request.requestURLHash());

        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
            boolean stored = cacheEntry.storeResponseData(data);
            if (!stored)
            {
                //TODO: Better handle if there is no slot available, For example, release response payload
                // which requests are in flight
                request.purge();
            }
            if (newEtag != null)
            {
                this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestURLHash());
            }
            sendWindow(data.length() + data.padding(), data.trace());
            break;
        case EndFW.TYPE_ID:
            final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
            checkEtag(end, cacheEntry);
            cacheEntry.setResponseCompleted(true);
            if (!this.streamFactory.defaultCache.isUpdatedByEtagToRetry(request, cacheEntry))
            {
                long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
                if (retryAfter <= 0L)
                {
                    retryCacheableRequest();
                }
                else
                {
                    this.streamFactory.scheduler.accept(retryAfter, this::retryCacheableRequest);
                }
            }
            else
            {
                this.streamFactory.defaultCache.signalForUpdatedCacheEntry(request.requestURLHash());
            }

            break;
        case AbortFW.TYPE_ID:
        default:
            this.streamFactory.defaultCache.signalAbort(request.requestURLHash());
            request.purge();
            break;
        }
    }

    private void checkEtag(EndFW end, DefaultCacheEntry cacheEntry)
    {
        final OctetsFW extension = end.extension();
        if (extension.sizeof() > 0)
        {
            final HttpEndExFW httpEndEx = extension.get(streamFactory.httpEndExRO::wrap);
            ListFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> ETAG.equals(h.name().asString()));
            if (etag != null)
            {
                newEtag = etag.value().asString();
                cacheEntry.setEtag(newEtag);
            }
        }
    }

    ///////////// PROXY
    private void doProxyBegin(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyId = streamCorrelation.acceptReplyId();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent proxy response]\n", currentTimeMillis(), acceptReplyId,
                    getHeader(responseHeaders, ":status"));
        }

        streamCorrelation.setThrottle(this::onThrottleMessageWhenProxying);
        streamFactory.writer.doHttpResponse(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                traceId,
                builder -> responseHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))
            ));

        // count all responses
        streamFactory.counters.responses.getAsLong();

        this.streamState = this::onStreamMessageWhenProxying;
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
            streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                    streamFactory.supplyTrace.getAsLong());
            break;
        }
    }

    public void onThrottleMessageWhenProxying(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
            onWindowWhenProxying(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = streamFactory.resetRO.wrap(buffer, index, index + length);
            onResetWhenProxying(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void sendWindow(int credit, long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                connectReplyStreamId, traceId, credit, padding, groupId);
        }
    }

    private void onDataWhenProxying(
        final DataFW data)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        final OctetsFW payload = data.payload();
        acceptReplyBudget -= payload.sizeof() + data.padding();
        assert acceptReplyBudget >= 0;
        streamFactory.writer.doHttpData(
            acceptReply,
            acceptRouteId,
            acceptReplyStreamId,
            data.trace(),
            data.groupId(),
            payload.buffer(),
            payload.offset(),
            payload.sizeof(),
            data.padding()
                                       );
    }

    private void onEndWhenProxying(
        final EndFW end)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();
        final long traceId = end.trace();
        streamFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyStreamId, traceId, end.extension());
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }

    private void onAbortWhenProxying(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        streamFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyStreamId, traceId);
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }

    private void onWindowWhenProxying(
        final WindowFW window)
    {
        final int credit = window.credit();
        padding = window.padding();
        groupId = window.groupId();
        acceptReplyBudget +=credit;
        streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                                      connectReplyStreamId, window.trace(), credit, padding, groupId);
    }

    private void onResetWhenProxying(
        final ResetFW reset)
    {
        streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, reset.trace());
        streamCorrelation.purge();
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }

    public void onThrottleBeforeBegin(
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
                    streamCorrelation.acceptReplyId(),
                    this.streamFactory.supplyTrace.getAsLong());
                streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
                streamCorrelation.purge();
                break;
        }
    }

    private void onSignal(SignalFW signal)
    {
        final long signalId = signal.signalId();

        if (this.streamCorrelation == null)
        {
            this.streamCorrelation = this.streamFactory.requestCorrelations.remove(connectReplyStreamId);
        }

        if (signalId == CACHE_ENTRY_UPDATED_SIGNAL || signalId == CACHE_ENTRY_SIGNAL)
        {
            DefaultRequest request = (DefaultRequest) streamCorrelation;
            cacheEntry = streamFactory.defaultCache.get(request.requestURLHash());
            if(payloadWritten == -1)
            {
                Future<?> requestExpiryTimeout = this.streamFactory.expiryRequestsCorrelations.remove(signal.streamId());
                if (requestExpiryTimeout != null)
                {
                    requestExpiryTimeout.cancel(true);
                }
                sendHttpResponseHeaders(cacheEntry, signalId);
            }
            else
            {
                this.streamFactory.budgetManager.resumeAssigningBudget(groupId, 0, signal.trace());
                sendEndIfNecessary(signal.trace());
            }
        }
        else if (signalId == REQUEST_EXPIRED_SIGNAL)
        {
            DefaultRequest request = (DefaultRequest) streamCorrelation;
            cacheEntry = streamFactory.defaultCache.get(request.requestURLHash());
            this.streamFactory.defaultCache.send304(cacheEntry, request);
        }
        else if (signalId == ABORT_SIGNAL)
        {
            streamFactory.writer.doAbort(streamCorrelation.acceptReply(),
                                        streamCorrelation.acceptRouteId(),
                                        streamCorrelation.acceptReplyStreamId,
                                        signal.trace());
            streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
            this.streamFactory.defaultCache.purge(cacheEntry);
        }
    }

    private void sendHttpResponseHeaders(DefaultCacheEntry cacheEntry, long signalId)
    {
        ListFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        final DefaultRequest request =(DefaultRequest)this.streamCorrelation;
        final MessageConsumer acceptReply = request.acceptReply();
        final long acceptRouteId = request.acceptRouteId();
        final long acceptReplyId = request.acceptReplyId();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId,
                              getHeader(responseHeaders, ":status"));
        }

        boolean isStale = false;
        if(signalId == CACHE_ENTRY_SIGNAL)
        {
            isStale = cacheEntry.isStale();
        }

        if (cacheEntry.etag() != null)
        {
            this.etagSent = true;
        }

        streamFactory.writer.doHttpResponseWithUpdatedHeaders(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            responseHeaders,
            cacheEntry.getRequestHeaders(this.streamFactory.requestHeadersRO),
            cacheEntry.etag(),
            isStale,

            this.streamFactory.supplyTrace.getAsLong());

        this.payloadWritten = 0;

        this.streamFactory.counters.responses.getAsLong();
    }

    private void sendEndIfNecessary(long traceId)
    {
        final DefaultRequest request = (DefaultRequest) streamCorrelation;
        final MessageConsumer acceptReply = request.acceptReply();
        final long acceptRouteId = request.acceptRouteId();
        final long acceptReplyStreamId = request.acceptReplyId();
        boolean ackedBudget = !this.streamFactory.budgetManager.hasUnackedBudget(groupId, acceptReplyStreamId);

        if (payloadWritten == cacheEntry.responseSize()
            && ackedBudget
            && cacheEntry.isResponseCompleted())
        {
            if (!etagSent && cacheEntry.etag() != null)
            {
                this.streamFactory.writer.doHttpEnd(acceptReply,
                                                    acceptRouteId,
                                                    acceptReplyStreamId,
                                                    traceId,
                                                    cacheEntry.etag());
            }
            else
            {
                this.streamFactory.writer.doHttpEnd(acceptReply,
                                                    acceptRouteId,
                                                    acceptReplyStreamId,
                                                    traceId);
            }

            this.streamFactory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                groupId,
                acceptReplyStreamId,
                traceId);
            this.streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
            this.streamFactory.defaultCache.removePendingInitialRequest(request);
            request.purge();
        }
    }

    private int writePayload(int budget, long trace)
    {
        DefaultRequest request = (DefaultRequest) streamCorrelation;
        final MessageConsumer acceptReply = request.acceptReply();
        final long acceptRouteId = request.acceptRouteId();
        final long acceptReplyStreamId = request.acceptReplyId();

        final int minBudget = min(budget, acceptReplyBudget);
        final int toWrite = min(minBudget - padding, cacheEntry.responseSize() - payloadWritten);
        if (toWrite > 0)
        {
            this.streamFactory.writer.doHttpData(
                acceptReply,
                acceptRouteId,
                acceptReplyStreamId,
                trace,
                groupId,
                padding,
                p -> this.buildResponsePayload(payloadWritten,
                                               toWrite,
                                               p,
                                               cacheEntry.getResponsePool())
            );
            payloadWritten += toWrite;
            budget -= (toWrite + padding);
            acceptReplyBudget -= (toWrite + padding);
            assert acceptReplyBudget >= 0;
        }

        return budget;
    }

    public void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    public void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder builder,
        BufferPool bp,
        int slotCnt)
    {
        if (length == 0)
        {
            return;
        }

        final int slotCapacity = bp.slotCapacity();
        int chunkedWrite = (slotCnt * slotCapacity) - index;
        int slot = cacheEntry.getResponseSlots().get(slotCnt);
        if (chunkedWrite > 0)
        {
            MutableDirectBuffer buffer = bp.buffer(slot);
            int offset = slotCapacity - chunkedWrite;
            int chunkLength = Math.min(chunkedWrite, length);
            builder.put(buffer, offset, chunkLength);
            index += chunkLength;
            length -= chunkLength;
        }
        buildResponsePayload(index, length, builder, bp, ++slotCnt);
    }
}
