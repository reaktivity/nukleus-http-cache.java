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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import java.time.Instant;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyCachedResponse
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer reply;
    private final long routeId;
    final long replyId;
    private final long authorization;
    private final DefaultCacheEntry cacheEntry;
    private final boolean promiseNextPollRequest;

    private int replyBudget;
    private int replyPadding;
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
        DefaultCacheEntry cacheEntry,
        boolean promiseNextPollRequest,
        Consumer<HttpCacheProxyCachedResponse> resetHandler)
    {
        this.factory = factory;
        this.reply = reply;
        this.routeId = routeId;
        this.replyId = replyId;
        this.authorization = authorization;
        this.cacheEntry = cacheEntry;
        this.promiseNextPollRequest = promiseNextPollRequest;
        this.resetHandler = resetHandler;
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
        ArrayFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        factory.router.setThrottle(replyId, this::onResponseMessage);
        final ArrayFW<HttpHeaderFW> requestHeaders = cacheEntry.getRequestHeaders();
        final String requestURL = getRequestURL(requestHeaders);
        factory.writer.doHttpResponseWithUpdatedHeaders(reply,
                                                        routeId,
                                                        replyId,
                                                        responseHeaders,
                                                        requestHeaders,
                                                        cacheEntry.etag(),
                                                        cacheEntry.isStale(now),
                                                        traceId);
        responseProgress = 0;

        factory.counters.responses.getAsLong();
    }

    void doResponseFlush(
        long traceId)
    {
        final int remaining = cacheEntry.responseSize() - responseProgress;
        final int writable = Math.min(replyBudget - replyPadding, remaining);

        if (writable > 0)
        {
            final int maximum = writable + replyPadding;
            final int minimum = Math.min(maximum, 1024);

            int claimed = maximum;
            if (replyDebitorIndex != NO_DEBITOR_INDEX)
            {
                claimed = replyDebitor.claim(replyDebitorIndex, replyId, minimum, maximum);
            }

            final int required = claimed;
            final int writableMax = required - replyPadding;
            if (writableMax > 0)
            {
                final BufferPool cacheResponsePool = factory.defaultCache.getResponsePool();

                factory.writer.doHttpData(
                    reply,
                    routeId,
                    replyId,
                    traceId,
                    replyDebitorId,
                    required,
                    p -> buildResponsePayload(responseProgress, writableMax, p, cacheResponsePool));

                responseProgress += writableMax;

                replyBudget -= required;
                assert replyBudget >= 0;
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
                               traceId);

        cleanupResponseIfNecessary();
        cacheEntry.setSubscribers(-1);
    }

    private void doResponseEnd(
        long traceId)
    {
        assert responseProgress == cacheEntry.responseSize();

        if (promiseNextPollRequest)
        {
            factory.counters.promises.getAsLong();
            factory.writer.doHttpPushPromise(reply,
                                             routeId,
                                             replyId,
                                             authorization,
                                             cacheEntry.getRequestHeaders(),
                                             cacheEntry.getCachedResponseHeaders(),
                                             cacheEntry.etag());
        }

        factory.writer.doHttpEnd(reply,
                                 routeId,
                                 replyId,
                                 traceId);

        cleanupResponseIfNecessary();
        cacheEntry.setSubscribers(-1);
    }

    private void onResponseReset(
        ResetFW reset)
    {
        cleanupResponseIfNecessary();
        cacheEntry.setSubscribers(-1);
        resetHandler.accept(this);
    }

    private void onResponseWindow(
        WindowFW window)
    {
        assert responseProgress != -1;
        final long traceId = window.traceId();

        replyDebitorId = window.budgetId();
        replyPadding = window.padding();
        int credit = window.credit();
        replyBudget += credit;

        if (replyDebitorId != 0L && replyDebitor == null)
        {
            replyDebitor = factory.supplyDebitor.apply(replyDebitorId);
            replyDebitorIndex = replyDebitor.acquire(replyDebitorId, replyId, this::doResponseFlush);
        }

        doResponseFlush(traceId);
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
