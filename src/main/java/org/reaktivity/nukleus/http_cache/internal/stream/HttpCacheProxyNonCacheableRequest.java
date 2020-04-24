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
    private final HttpCacheProxyNonCacheableResponse nonCacheableResponse;

    private final MessageConsumer initial;
    private final long routeId;
    private final long initialId;
    final long replyId;

    private final MessageConsumer connectInitial;
    private final long connectRouteId;
    private final long connectInitialId;

    final long connectReplyId;

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
        this.replyId = factory.supplyReplyId.applyAsLong(initialId);
        this.connectRouteId = resolveId;
        this.connectInitialId = factory.supplyInitialId.applyAsLong(resolveId);
        this.connectInitial = factory.router.supplyReceiver(connectInitialId);
        this.connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);

        nonCacheableResponse =
            new HttpCacheProxyNonCacheableResponse(factory,
                requestHash,
                requestURL,
                isMethodUnsafe,
                factory.router.supplyReceiver(connectReplyId),
                connectRouteId,
                connectReplyId,
                initial,
                routeId,
                replyId);
        factory.router.setThrottle(replyId, nonCacheableResponse::onResponseMessage);
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        return nonCacheableResponse::onResponseMessage;
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
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(factory.httpBeginExRO::tryWrap);
        assert httpBeginEx != null;
        final ArrayFW<HttpHeaderFW> headers = httpBeginEx.headers();

        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
        factory.writer.doHttpRequest(
            connectInitial,
            connectRouteId,
            connectInitialId,
            factory.supplyTraceId.getAsLong(),
            0L,
            hs -> headers.forEach(h -> hs.item(item -> item.name(h.name()).value(h.value()))));
        factory.correlations.put(connectReplyId, this::newResponse);
    }

    private void onRequestData(
        final DataFW data)
    {
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final OctetsFW payload = data.payload();

        factory.writer.doHttpData(connectInitial,
                                  connectRouteId,
                                  connectInitialId,
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
        final long traceId = end.traceId();
        factory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, traceId);
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        final long traceId = abort.traceId();
        factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int credit = window.credit();
        final int padding = window.padding();

        factory.writer.doWindow(initial, routeId, initialId, traceId, budgetId, credit, padding);
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        final long traceId = reset.traceId();

        factory.writer.doReset(initial, routeId, initialId, traceId);
    }
}
