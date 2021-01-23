/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;

import java.time.Instant;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyCachedResponse
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer reply;
    private final DefaultCacheEntry cacheEntry;
    private final long routeId;
    private final long replyId;
    private final long authorization;
    private final boolean promiseNextPollRequest;

    private long replySeq;
    private long replyAck;
    private int replyMax;
    private int replyPad;
    private long replyDebitorId;
    private BudgetDebitor replyDebitor;
    private long replyDebitorIndex = NO_DEBITOR_INDEX;

    private int responseProgress = -1;
    private Consumer<HttpCacheProxyCachedResponse> resetHandler;

    HttpCacheProxyCachedResponse(
        HttpCacheProxyFactory factory,
        MessageConsumer reply,
        long routeId,
        long replyId,
        long authorization,
        long replyBudgetId,
        long replySeq,
        long replyAck,
        int replyMax,
        int replyPad,
        int requestHash,
        boolean promiseNextPollRequest,
        Consumer<HttpCacheProxyCachedResponse> resetHandler)
    {
        this.factory = factory;
        this.reply = reply;
        this.routeId = routeId;
        this.replyId = replyId;
        this.authorization = authorization;
        this.cacheEntry = factory.defaultCache.lookup(requestHash);
        this.promiseNextPollRequest = promiseNextPollRequest;
        this.resetHandler = resetHandler;
        updateBudget(replyBudgetId, replySeq, replyAck, replyMax, replyPad);
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
            onResponseReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onResponseWindow(window);
            break;
        case SignalFW.TYPE_ID:
            assert false;
            break;
        default:
            break;
        }
    }

    void doResponseBegin(
        Instant now,
        long traceId)
    {
        Array32FW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        factory.router.setThrottle(replyId, this::onResponseMessage);
        final Array32FW<HttpHeaderFW> requestHeaders = cacheEntry.getRequestHeaders();
        factory.writer.doHttpResponseWithUpdatedHeaders(reply,
                                                        routeId,
                                                        replyId,
                                                        replySeq,
                                                        replyAck,
                                                        replyMax,
                                                        responseHeaders,
                                                        requestHeaders,
                                                        cacheEntry.etag(),
                                                        cacheEntry.isStale(now),
                                                        traceId);
        responseProgress = 0;
        doResponseFlush(traceId);

        factory.counters.responses.getAsLong();
        factory.counters.responsesCached.getAsLong();
    }

    void doResponseFlush(
        long traceId)
    {
        final int remaining = cacheEntry.responseSize() - responseProgress;
        final int replyNoAck = (int)(replySeq - replyAck);
        final int writable = Math.min(replyMax - replyNoAck - replyPad, remaining);

        if (writable > 0)
        {
            final int maximum = writable + replyPad;
            final int minimum = Math.min(maximum, 1024 + replyPad);

            int claimed = maximum;
            if (replyDebitorIndex != NO_DEBITOR_INDEX)
            {
                claimed = replyDebitor.claim(traceId, replyDebitorIndex, replyId, minimum, maximum, 0);
            }

            final int reserved = claimed;
            final int writableMax = reserved - replyPad;
            if (writableMax > 0)
            {
                final BufferPool cacheResponsePool = factory.defaultCache.getResponsePool();

                factory.writer.doHttpData(
                    reply,
                    routeId,
                    replyId,
                    replySeq,
                    replyAck,
                    replyMax,
                    traceId,
                    replyDebitorId,
                    reserved,
                    p -> buildResponsePayload(responseProgress, writableMax, p, cacheResponsePool));

                responseProgress += writableMax;

                replySeq += reserved;

                assert replyAck <= replySeq;
            }
        }

        if (cacheEntry.isResponseCompleted() && responseProgress == cacheEntry.responseSize())
        {
            doResponseEnd(traceId);
        }
    }

    void doResponseAbort(
        long traceId)
    {
        factory.writer.doAbort(reply,
                               routeId,
                               replyId,
                               replySeq,
                               replyAck,
                               replyMax,
                               traceId);

        cleanupResponseIfNecessary();
    }

    private void doResponseEnd(
        long traceId)
    {
        assert responseProgress == cacheEntry.responseSize();
        final Array32FW<HttpHeaderFW> cachedResponseHeaders = cacheEntry.getCachedResponseHeaders();
        int freshnessExtension = SurrogateControl.getSurrogateFreshnessExtension(cachedResponseHeaders);

        if (promiseNextPollRequest && freshnessExtension > 0)
        {
            factory.counters.promises.getAsLong();
            factory.writer.doHttpPushPromise(reply,
                                             routeId,
                                             replyId,
                                             replySeq,
                                             replyAck,
                                             replyMax,
                                             authorization,
                                             cacheEntry.getRequestHeaders(),
                                             cachedResponseHeaders,
                                             cacheEntry.etag());
        }

        factory.writer.doHttpEnd(reply,
                                 routeId,
                                 replyId,
                                 replySeq,
                                 replyAck,
                                 replyMax,
                                 traceId);

        cleanupResponseIfNecessary();
        resetHandler.accept(this);
    }

    private void onResponseReset(
        ResetFW reset)
    {
        cleanupResponseIfNecessary();
        resetHandler.accept(this);
    }

    private void onResponseWindow(
        WindowFW window)
    {
        final long traceId = window.traceId();

        updateBudget(window.budgetId(), window.sequence(), window.acknowledge(), window.maximum(), window.padding());

        doResponseFlush(traceId);
    }

    private void updateBudget(
        long budgetId,
        long sequence,
        long acknowledge,
        int maximum,
        int padding)
    {
        replyDebitorId = budgetId;
        replySeq = sequence;
        replyAck = acknowledge;
        replyMax = maximum;
        replyPad = padding;

        if (replyDebitorId != 0L && replyDebitor == null)
        {
            replyDebitor = factory.supplyDebitor.apply(replyDebitorId);
            replyDebitorIndex = replyDebitor.acquire(replyDebitorId, replyId, this::doResponseFlush);
        }
    }

    private void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    private void buildResponsePayload(
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

    private void cleanupResponseIfNecessary()
    {
        if (replyDebitorIndex != NO_DEBITOR_INDEX)
        {
            replyDebitor.release(replyDebitorIndex, replyId);
            replyDebitorIndex = NO_DEBITOR_INDEX;
            replyDebitor = null;
        }
    }
}
