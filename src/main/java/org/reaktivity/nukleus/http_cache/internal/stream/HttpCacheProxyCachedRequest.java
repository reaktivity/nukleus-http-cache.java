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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
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

import java.util.function.Function;

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

final class HttpCacheProxyCachedRequest
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptReplyId;
    private final MessageConsumer signaler;
    private final Function<Long, MessageConsumer> supplyReceiver;

    private final int requestHash;

    private int acceptReplyBudget;
    private long groupId;
    private int padding;

    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagMatched;

    HttpCacheProxyCachedRequest(
        HttpCacheProxyFactory factory,
        int requestHash,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        long acceptInitialId)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.supplyReceiver = factory.router::supplyReceiver;
        this.signaler = supplyReceiver.apply(acceptInitialId);
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
            case WindowFW.TYPE_ID:
                final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

        // count all requests
        factory.counters.requests.getAsLong();
        factory.counters.requestsCacheable.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [received request]\n",
                    currentTimeMillis(), acceptReplyId, getRequestURL(httpBeginFW.headers()));
        }

        DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestHash);
        final String requestAuthorizationHeader = getHeader(requestHeaders, AUTHORIZATION);
        cacheEntry.recentAuthorizationHeader(requestAuthorizationHeader);

        etagMatched = CacheUtils.isMatchByEtag(requestHeaders, cacheEntry.etag());
        if (etagMatched)
        {
            if (DEBUG)
            {
                System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n",
                                  currentTimeMillis(), acceptReplyId, "304");
            }

            factory.writer.do304(acceptReply,
                                 acceptRouteId,
                                 acceptReplyId,
                                 factory.supplyTrace.getAsLong());
        }
        else
        {
            factory.writer.doSignal(signaler,
                                    acceptRouteId,
                                    acceptReplyId,
                                    factory.supplyTrace.getAsLong(),
                                    CACHE_ENTRY_SIGNAL);
        }
    }

    private void onData(
        final DataFW data)
    {
        //NOOP
    }

    private void onEnd(
        final EndFW end)
    {
        if(etagMatched)
        {
            factory.writer.doHttpEnd(acceptReply,
                                     acceptRouteId,
                                     acceptReplyId,
                                     factory.supplyTrace.getAsLong());
        }
    }

    private void onAbort(
        final AbortFW abort)
    {
        if(etagMatched)
        {
            factory.writer.doAbort(acceptReply,
                                   acceptRouteId,
                                   acceptReplyId,
                                   factory.supplyTrace.getAsLong());
        }
        else
        {
            factory.writer.doSignal(signaler,
                                    acceptRouteId,
                                    acceptReplyId,
                                    factory.supplyTrace.getAsLong(),
                                    ABORT_SIGNAL);
        }
    }

    private void onSignal(
        SignalFW signal)
    {
        final long signalId = signal.signalId();

        if (signalId == CACHE_ENTRY_SIGNAL)
        {
            handleCacheUpdateSignal(signal);
        }
        else if (signalId == ABORT_SIGNAL)
        {
            factory.writer.doAbort(acceptReply,
                                   acceptRouteId,
                                   acceptReplyId,
                                   signal.trace());
        }
    }

    private void onWindow(WindowFW window)
    {
        groupId = window.groupId();
        padding = window.padding();
        long streamId = window.streamId();
        int credit = window.credit();
        acceptReplyBudget += credit;
        factory.budgetManager.window(BudgetManager.StreamKind.CACHE,
                                     groupId,
                                     streamId,
                                     credit,
                                     this::writePayload,
                                     window.trace());
        sendEndIfNecessary(window.trace());
    }

    private void onReset(ResetFW reset)
    {
        factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                     groupId,
                                     acceptReplyId,
                                     this.factory.supplyTrace.getAsLong());
    }

    private void handleCacheUpdateSignal(
        SignalFW signal)
    {
        cacheEntry = factory.defaultCache.get(requestHash);
        if (cacheEntry == null)
        {
            return;
        }
        sendHttpResponseHeaders(cacheEntry, signal.signalId());
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

        factory.writer.doHttpResponseWithUpdatedHeaders(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            responseHeaders,
            cacheEntry.getRequestHeaders(),
            cacheEntry.etag(),
            isStale,

            factory.supplyTrace.getAsLong());

        payloadWritten = 0;

        factory.counters.responses.getAsLong();
        factory.defaultCache.counters.responsesCached.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {

        boolean ackedBudget = !factory.budgetManager.hasUnackedBudget(groupId, acceptReplyId);

        if (payloadWritten == cacheEntry.responseSize()
            && ackedBudget)
        {
            factory.writer.doHttpEnd(acceptReply,
                                     acceptRouteId,
                                     acceptReplyId,
                                     traceId);

            factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                   groupId,
                                                   acceptReplyId,
                                                   traceId);
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
            factory.writer.doHttpData(acceptReply,
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
}
