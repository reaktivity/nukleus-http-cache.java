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
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
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

    private final boolean isMethodUnsafe;
    private final MessageConsumer connect;
    private final long connectRouteId;
    private final long connectReplyId;

    private final MessageConsumer accept;
    private final long acceptRouteId;
    private final long acceptReplyId;
    private final long replyBudgetId;

    private long replySeq;
    private long replyAck;
    private int replyMax;
    private int replyPad;

    HttpCacheProxyNonCacheableResponse(
        HttpCacheProxyFactory factory,
        int requestHash,
        String requestURL,
        boolean isMethodUnsafe,
        MessageConsumer connect,
        long connectRouteId,
        long connectReplyId,
        MessageConsumer accept,
        long acceptRouteId,
        long acceptReplyId,
        long replyBudgetId,
        long replySeq,
        long replyAck,
        int replyMax,
        int replyPad)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.isMethodUnsafe = isMethodUnsafe;
        this.connect = connect;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.accept = accept;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.replyBudgetId = replyBudgetId;
        this.replySeq = replySeq;
        this.replyAck = replyAck;
        this.replyMax = replyMax;
        this.replyPad = replyPad;
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
            onResponseBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onResponseData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onResponseEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onResponseAbort(abort);
            break;
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

    private void onResponseBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(factory.httpBeginExRO::tryWrap);
        final HttpBeginExFW httpBeginExFinal = (httpBeginEx == null) ? factory.defaultHttpBeginExRO : httpBeginEx;

        final long traceId = begin.traceId();
        final Array32FW<HttpHeaderFW> headers = httpBeginExFinal.headers();

        factory.writer.doHttpResponse(
            accept,
            acceptRouteId,
            acceptReplyId,
            replySeq,
            replyAck,
            replyMax,
            traceId,
            builder -> headers.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))));

        // count all responses
        factory.counters.responses.getAsLong();

        if (isMethodUnsafe)
        {
            factory.defaultCache.invalidateCacheEntryIfNecessary(factory, requestHash, requestURL, traceId, headers);
        }

        if (replySeq > 0 || replyAck > 0 || replyMax > 0 || replyPad > 0)
        {
            factory.writer.doWindow(
                connect,
                connectRouteId,
                connectReplyId,
                replySeq,
                replyAck,
                replyMax,
                traceId,
                replyBudgetId,
                replyPad);
        }
    }

    private void onResponseData(
        final DataFW data)
    {
        final long sequence = data.sequence();
        final long acknowledge = data.acknowledge();
        final int maximum = data.maximum();
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final OctetsFW payload = data.payload();

        assert acknowledge <= sequence;
        assert sequence >= replySeq;

        replySeq = sequence + reserved;

        assert replyAck <= replySeq;

        factory.writer.doHttpData(accept,
                                  acceptRouteId,
                                  acceptReplyId,
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

    private void onResponseEnd(
        final EndFW end)
    {
        final long sequence = end.sequence();
        final long acknowledge = end.acknowledge();
        final int maximum = end.maximum();
        final long traceId = end.traceId();
        final OctetsFW extension = end.extension();

        factory.writer.doHttpEnd(accept, acceptRouteId, acceptReplyId, sequence, acknowledge, maximum, traceId, extension);
    }

    private void onResponseAbort(
        final AbortFW abort)
    {
        final long sequence = abort.sequence();
        final long acknowledge = abort.acknowledge();
        final int maximum = abort.maximum();
        final long traceId = abort.traceId();

        factory.writer.doAbort(accept, acceptRouteId, acceptReplyId, sequence, acknowledge, maximum, traceId);
    }

    private void onResponseWindow(
        final WindowFW window)
    {
        final long sequence = window.sequence();
        final long acknowledge = window.acknowledge();
        final int maximum = window.maximum();
        final long traceId = window.traceId();
        final long budgetId = window.budgetId();
        final int padding = window.padding();

        assert acknowledge <= sequence;
        assert sequence <= replySeq;
        assert acknowledge >= replyAck;
        assert maximum >= replyMax;

        this.replyAck = acknowledge;
        this.replyMax = maximum;
        this.replyPad = padding;

        factory.writer.doWindow(connect,
                                connectRouteId,
                                connectReplyId,
                                sequence,
                                acknowledge,
                                maximum,
                                traceId,
                                budgetId,
                                padding);
    }

    private void onResponseReset(
        final ResetFW reset)
    {
        final long sequence = reset.sequence();
        final long acknowledge = reset.acknowledge();
        final int maximum = reset.maximum();
        final long traceId = reset.traceId();

        factory.writer.doReset(connect,
                               connectRouteId,
                               connectReplyId,
                               sequence,
                               acknowledge,
                               maximum,
                               traceId);
    }
}
