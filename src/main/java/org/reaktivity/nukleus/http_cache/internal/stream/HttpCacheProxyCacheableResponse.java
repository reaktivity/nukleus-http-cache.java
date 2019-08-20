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

import java.util.concurrent.Future;

final class HttpCacheProxyCacheableResponse
{
    private final HttpCacheProxyFactory httpCacheProxyFactory;
    private final HttpCacheProxyCacheableRequest request;

    private int connectReplyBudget;
    private int acceptReplyBudget;
    private long groupId;
    private int padding;

    private final int requestSlot;
    private final int initialWindow;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private long traceId;
    private boolean isResponseBuffering;

    private int requestHash;
    public final MessageConsumer acceptReply;
    public final long acceptRouteId;
    public final long acceptStreamId;
    public final long acceptReplyId;

    public MessageConsumer connectReply;
    public long connectRouteId;
    public long connectReplyId;


    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        int requestHash,
        int requestSlot,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptStreamId,
        long acceptReplyId,
        MessageConsumer connectReply,
        long connectReplyId,
        long connectRouteId,
        HttpCacheProxyCacheableRequest request)
    {
        this.httpCacheProxyFactory = httpCacheProxyFactory;
        this.requestSlot = requestSlot;
        this.requestHash = requestHash;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptStreamId = acceptStreamId;
        this.acceptReplyId = acceptReplyId;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.request = request;
        this.initialWindow = httpCacheProxyFactory.responseBufferPool.slotCapacity();
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, acceptReplyBudget=%016x, " +
                "connectReplyBudget=%d, padding=%d]", getClass().getSimpleName(),
            connectRouteId, connectReplyId, acceptReplyBudget, connectReplyBudget, padding);
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
            case BeginFW.TYPE_ID:
                final BeginFW begin = httpCacheProxyFactory.beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = httpCacheProxyFactory.dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = httpCacheProxyFactory.endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = httpCacheProxyFactory.abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = httpCacheProxyFactory.windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = httpCacheProxyFactory.signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = httpCacheProxyFactory.resetRO.wrap(buffer, index, index + length);
                onResponseReset(reset);
                break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        traceId = begin.trace();

        final OctetsFW extension = httpCacheProxyFactory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(httpCacheProxyFactory.httpBeginExRO::wrap);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                    getHeader(httpBeginFW.headers(), ":status"));
        }

        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        DefaultCacheEntry cacheEntry = httpCacheProxyFactory.defaultCache.supply(requestHash);
        String newEtag = getHeader(responseHeaders, ETAG);

        if (newEtag == null)
        {
            isResponseBuffering = true;
        }

        //Initial cache entry
        if(cacheEntry.etag() == null && cacheEntry.requestHeadersSize() == 0)
        {
            if (!cacheEntry.storeRequestHeaders(getRequestHeaders())
                || !cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //TODO: Better handle if there is no slot available, For example, release response payload
                // which requests are in flight
                request.purge();
            }
            if(!isResponseBuffering)
            {
                httpCacheProxyFactory.defaultCache.signalForUpdatedCacheEntry(requestHash);
            }

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
            if(!isResponseBuffering)
            {
                httpCacheProxyFactory.defaultCache.signalForUpdatedCacheEntry(requestHash);
            }
        }

        sendWindow(initialWindow, traceId);
    }

    private void onData(
        DataFW data)
    {
        DefaultCacheEntry cacheEntry = httpCacheProxyFactory.defaultCache.get(requestHash);
        boolean stored = cacheEntry.storeResponseData(data);
        if (!stored)
        {
            //TODO: Better handle if there is no slot available, For example, release response payload
            // which requests are in flight
            request.purge();
        }
        if (!isResponseBuffering)
        {
            httpCacheProxyFactory.defaultCache.signalForUpdatedCacheEntry(requestHash);
        }
        sendWindow(data.length() + data.padding(), data.trace());
    }

    private void onEnd(
        EndFW end)
    {
        DefaultCacheEntry cacheEntry = this.httpCacheProxyFactory.defaultCache.get(requestHash);

        checkEtag(end, cacheEntry);
        cacheEntry.setResponseCompleted(true);

        if (!httpCacheProxyFactory.defaultCache.isUpdatedByEtagToRetry(request, cacheEntry))
        {
            long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
            request.scheduleRequest(retryAfter);
        }
        else
        {
            httpCacheProxyFactory.defaultCache.signalForUpdatedCacheEntry(requestHash);
            httpCacheProxyFactory.defaultCache.removeAllPendingInitialRequests(requestHash);
        }
    }

    private void onAbort(AbortFW abort)
    {
        if (isResponseBuffering)
        {
            httpCacheProxyFactory.writer.do503AndAbort(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       abort.trace(),
                                                       0L);
            httpCacheProxyFactory.defaultCache.serveNextPendingInitialRequest(request);
        }
        else
        {
            httpCacheProxyFactory.defaultCache.signalAbortAllSubscribers(requestHash);
        }
        request.purge();
        httpCacheProxyFactory.defaultCache.purge(requestHash);
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
            httpCacheProxyFactory.defaultCache.send304ToPendingInitialRequests(requestHash);
        }
        else if (signalId == ABORT_SIGNAL)
        {
            if (this.payloadWritten >= 0)
            {
                httpCacheProxyFactory.writer.doAbort(acceptReply,
                                                     acceptRouteId,
                                                     acceptReplyId,
                                                     signal.trace());
            }
            else
            {
                send503RetryAfter();
            }
        }
    }

    private void onWindow(WindowFW window)
    {
        groupId = window.groupId();
        padding = window.padding();
        long streamId = window.streamId();
        int credit = window.credit();
        acceptReplyBudget += credit;
        httpCacheProxyFactory.budgetManager.window(BudgetManager.StreamKind.CACHE,
                                                   groupId,
                                                   streamId,
                                                   credit,
                                                   this::writePayload,
                                                   window.trace());
        sendEndIfNecessary(window.trace());
    }

    private void onResponseReset(ResetFW reset)
    {
        httpCacheProxyFactory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                   groupId,
                                                   acceptReplyId,
                                                   this.httpCacheProxyFactory.supplyTrace.getAsLong());
        httpCacheProxyFactory.defaultCache.removePendingInitialRequest(request);
        request.purge();
        httpCacheProxyFactory.writer.doReset(acceptReply,
                                             acceptRouteId,
                                             acceptStreamId,
                                             httpCacheProxyFactory.supplyTrace.getAsLong());
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(),
                              acceptReplyId, "503");
        }

        httpCacheProxyFactory.writer.doHttpResponse(acceptReply,
                                                    acceptRouteId,
                                                    acceptReplyId,
                                                    httpCacheProxyFactory.supplyTrace.getAsLong(), e ->
                                            e.item(h -> h.name(STATUS).value(SERVICE_UNAVAILABLE_503))
                                             .item(h -> h.name("retry-after").value("0")));
        httpCacheProxyFactory.writer.doHttpEnd(acceptReply,
                                               acceptRouteId,
                                               acceptReplyId,
                                               httpCacheProxyFactory.supplyTrace.getAsLong());

        // count all responses
        httpCacheProxyFactory.counters.responses.getAsLong();

        // count retry responses
        httpCacheProxyFactory.counters.responsesRetry.getAsLong();
    }

    private void handleCacheUpdateSignal(
        SignalFW signal)
    {
        cacheEntry = httpCacheProxyFactory.defaultCache.get(requestHash);
        if (cacheEntry == null)
        {
            return;
        }
        if(payloadWritten == -1)
        {
            Future<?> requestExpiryTimeout = httpCacheProxyFactory.expiryRequestsCorrelations.remove(signal.streamId());
            if (requestExpiryTimeout != null)
            {
                requestExpiryTimeout.cancel(true);
            }
            sendHttpResponseHeaders(cacheEntry, signal.signalId());
        }
        else
        {
            httpCacheProxyFactory.budgetManager.resumeAssigningBudget(groupId, 0, signal.trace());
            sendEndIfNecessary(signal.trace());
        }
    }

    private void sendHttpResponseHeaders(
        DefaultCacheEntry cacheEntry,
        long signalId)
    {
        ListFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

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
            etagSent = true;
        }

        httpCacheProxyFactory.writer.doHttpResponseWithUpdatedHeaders(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            responseHeaders,
            cacheEntry.getRequestHeaders(httpCacheProxyFactory.requestHeadersRO),
            cacheEntry.etag(),
            isStale,

            httpCacheProxyFactory.supplyTrace.getAsLong());

        payloadWritten = 0;

        httpCacheProxyFactory.counters.responses.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {

        boolean ackedBudget = !httpCacheProxyFactory.budgetManager.hasUnackedBudget(groupId, acceptReplyId);

        if (payloadWritten == cacheEntry.responseSize()
            && ackedBudget
            && cacheEntry.isResponseCompleted())
        {
            if (!etagSent && cacheEntry.etag() != null)
            {
                httpCacheProxyFactory.writer.doHttpEnd(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       traceId,
                                                       cacheEntry.etag());
            }
            else
            {
                httpCacheProxyFactory.writer.doHttpEnd(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       traceId);
            }

            httpCacheProxyFactory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                       groupId,
                                                       acceptReplyId,
                                                       traceId);
            httpCacheProxyFactory.defaultCache.removePendingInitialRequest(request);
            request.purge();
        }
    }

    private int writePayload(
        int budget,
        long trace)
    {
        final int minBudget = min(budget, acceptReplyBudget);
        final int toWrite = min(minBudget - padding, cacheEntry.responseSize() - payloadWritten);
        if (toWrite > 0)
        {
            httpCacheProxyFactory.writer.doHttpData(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                trace,
                groupId,
                padding,
                p -> buildResponsePayload(payloadWritten,
                                               toWrite,
                                               p,
                                               cacheEntry.getResponsePool()));
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

    private void checkEtag(
        EndFW end,
        DefaultCacheEntry cacheEntry)
    {
        final OctetsFW extension = end.extension();
        if (extension.sizeof() > 0)
        {
            final HttpEndExFW httpEndEx = extension.get(httpCacheProxyFactory.httpEndExRO::wrap);
            ListFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> ETAG.equals(h.name().asString()));
            if (etag != null)
            {
                cacheEntry.setEtag(etag.value().asString());
                isResponseBuffering = false;
            }
        }
    }

    private void sendWindow(
        int credit,
        long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            httpCacheProxyFactory.writer.doWindow(connectReply,
                                                  connectRouteId,
                                                  connectReplyId,
                                                  traceId,
                                                  credit,
                                                  padding,
                                                  groupId);
        }
    }

    public ListFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = httpCacheProxyFactory.requestBufferPool.buffer(requestSlot);
        return httpCacheProxyFactory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }
}
