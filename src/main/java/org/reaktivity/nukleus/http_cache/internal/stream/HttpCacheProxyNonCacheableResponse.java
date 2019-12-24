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

final class HttpCacheProxyNonCacheableResponse
{
    private final HttpCacheProxyFactory factory;

    private final int requestHash;
    private final String requestURL;

    private final MessageConsumer connect;
    private final long connectRouteId;
    private final long connectReplyId;

    private final MessageConsumer accept;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private int acceptReplyBudget;

    HttpCacheProxyNonCacheableResponse(
        HttpCacheProxyFactory factory,
        int requestHash,
        String requestURL,
        MessageConsumer connect,
        long connectRouteId,
        long connectReplyId,
        MessageConsumer accept,
        long acceptRouteId,
        long acceptReplyId)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.connect = connect;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.accept = accept;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d]",
                             getClass().getSimpleName(), connectRouteId, connectReplyId);
    }

    void onResponseMessage(
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
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onWindow(window);
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
        final long traceId = begin.traceId();

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::tryWrap);
        assert httpBeginFW != null;
        final ArrayFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        factory.writer.doHttpResponse(
            accept,
            acceptRouteId,
            acceptReplyId,
            traceId,
            builder -> responseHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))));

        // count all responses
        factory.counters.responses.getAsLong();

        factory.defaultCache.invalidateCacheEntryIfNecessary(factory,
                                                             requestHash,
                                                             requestURL,
                                                             traceId,
                                                             responseHeaders);
    }

    private void onData(
        final DataFW data)
    {
        final OctetsFW payload = data.payload();
        acceptReplyBudget -= data.reserved();
        assert acceptReplyBudget >= 0;

        factory.writer.doHttpData(accept,
                                  acceptRouteId,
                                  acceptReplyId,
                                  data.traceId(),
                                  data.budgetId(),
                                  payload.buffer(),
                                  payload.offset(),
                                  payload.sizeof(),
                                  data.reserved());
    }

    private void onEnd(
        final EndFW end)
    {
        final long traceId = end.traceId();
        factory.writer.doHttpEnd(accept, acceptRouteId, acceptReplyId, traceId, end.extension());
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.traceId();
        factory.writer.doAbort(accept, acceptRouteId, acceptReplyId, traceId);
    }

    private void onWindow(
        final WindowFW window)
    {
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int credit = window.credit();
        final int padding = window.padding();
        acceptReplyBudget += credit;
        factory.writer.doWindow(connect,
                                connectRouteId,
                                connectReplyId,
                                traceId,
                                budgetId,
                                credit,
                                padding);
    }

    private void onReset(
        final ResetFW reset)
    {
        factory.writer.doReset(connect,
                               connectRouteId,
                               connectReplyId,
                               reset.traceId());
    }
}
