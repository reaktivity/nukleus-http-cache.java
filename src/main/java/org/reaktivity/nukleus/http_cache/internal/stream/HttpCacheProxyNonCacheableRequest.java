/**
 * Copyright 2016-2021 The Reaktivity Project
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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyNonCacheableRequest
{
    private final HttpCacheProxyFactory factory;

    private final MessageConsumer initial;
    private final long routeId;
    private final long initialId;
    private final boolean isMethodUnsafe;
    final long replyId;

    private final MessageConsumer connectInitial;
    private final long connectRouteId;
    private final long connectInitialId;

    private final MessageConsumer connectReply;
    final long connectReplyId;

    private final String requestURL;
    private final int requestHash;
    private long replyBudgetId;
    private long replySeq;
    private long replyAck;
    private int replyMax;
    private int replyPad;

    HttpCacheProxyNonCacheableRequest(
        HttpCacheProxyFactory factory,
        MessageConsumer initial,
        long routeId,
        long initialId,
        long resolveId,
        int requestHash,
        String requestURL,
        boolean isMethodUnsafe)
    {
        this.factory = factory;
        this.initial = initial;
        this.routeId = routeId;
        this.initialId = initialId;
        this.isMethodUnsafe = isMethodUnsafe;
        this.replyId = factory.supplyReplyId.applyAsLong(initialId);
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.connectRouteId = resolveId;
        this.connectInitialId = factory.supplyInitialId.applyAsLong(resolveId);
        this.connectInitial = factory.router.supplyReceiver(connectInitialId);
        this.connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        this.connectReply = factory.router.supplyReceiver(connectReplyId);
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        final HttpCacheProxyNonCacheableResponse nonCacheableResponse =
            new HttpCacheProxyNonCacheableResponse(
                factory,
                requestHash,
                requestURL,
                isMethodUnsafe,
                connectReply,
                connectRouteId,
                connectReplyId,
                initial,
                routeId,
                replyId,
                replyBudgetId,
                replySeq,
                replyAck,
                replyMax,
                replyPad);
        factory.router.setThrottle(replyId, nonCacheableResponse::onResponseMessage);
        return nonCacheableResponse::onResponseMessage;
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
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onResponseReset(reset);
            break;
        }
    }

    private void onResponseWindow(
        WindowFW window)
    {
        replyBudgetId = window.budgetId();
        replySeq = window.sequence();
        replyAck = window.acknowledge();
        replyMax = window.maximum();
        replyPad = window.padding();
    }

    private void onResponseReset(
        ResetFW reset)
    {
        final long sequence = reset.sequence();
        final long acknowledge = reset.acknowledge();
        final int maximum = reset.maximum();
        final long traceId = reset.traceId();

        factory.writer.doReset(connectInitial,
            connectRouteId,
            connectReplyId,
            sequence,
            acknowledge,
            maximum,
            traceId);
        factory.correlations.remove(connectReplyId);
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
            onRequestBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onRequestData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onRequestEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onRequestAbort(abort);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onRequestWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onRequestReset(reset);
            break;
        default:
            break;
        }
    }

    private void onRequestBegin(
        BeginFW begin)
    {
        final long sequence = begin.sequence();
        final long acknowledge = begin.acknowledge();
        final int maximum = begin.maximum();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(factory.httpBeginExRO::tryWrap);
        assert httpBeginEx != null;
        final ArrayFW<HttpHeaderFW> headers = httpBeginEx.headers();

        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
        factory.writer.doHttpRequest(
            connectInitial,
            connectRouteId,
            connectInitialId,
            sequence,
            acknowledge,
            maximum,
            factory.supplyTraceId.getAsLong(),
            0L,
            hs -> headers.forEach(h -> hs.item(item -> item.name(h.name()).value(h.value()))));
        factory.correlations.put(connectReplyId, this::newResponse);
    }

    private void onRequestData(
        final DataFW data)
    {
        final long sequence = data.sequence();
        final long acknowledge = data.acknowledge();
        final int maximum = data.maximum();
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final OctetsFW payload = data.payload();

        factory.writer.doHttpData(
            connectInitial,
            connectRouteId,
            connectInitialId,
            sequence,
            acknowledge,
            maximum,
            traceId,
            budgetId,
            payload.buffer(),
            payload.offset(),
            payload.sizeof(),
            reserved);
    }

    private void onRequestEnd(
        final EndFW end)
    {
        final long sequence = end.sequence();
        final long acknowledge = end.acknowledge();
        final int maximum = end.maximum();
        final long traceId = end.traceId();
        factory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, sequence, acknowledge, maximum, traceId);
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        final long sequence = abort.sequence();
        final long acknowledge = abort.acknowledge();
        final int maximum = abort.maximum();
        final long traceId = abort.traceId();
        factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, sequence, acknowledge, maximum, traceId);
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        final long sequence = window.sequence();
        final long acknowledge = window.acknowledge();
        final int maximum = window.maximum();
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int padding = window.padding();

        factory.writer.doWindow(initial, routeId, initialId, sequence, acknowledge, maximum, traceId, budgetId, padding);
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        final long sequence = reset.sequence();
        final long acknowledge = reset.acknowledge();
        final int maximum = reset.maximum();
        final long traceId = reset.traceId();

        factory.writer.doReset(initial, routeId, initialId, sequence, acknowledge, maximum, traceId);
    }
}
