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

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

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

    private final MessageConsumer connect;
    private final long connectRouteId;
    private final long connectReplyId;

    private final MessageConsumer accept;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private int acceptReplyBudget;

    HttpCacheProxyNonCacheableResponse(
        HttpCacheProxyFactory factory,
        MessageConsumer connect,
        long connectRouteId,
        long connectReplyId,
        MessageConsumer accept,
        long acceptRouteId,
        long acceptReplyId)
    {
        this.factory = factory;
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
        final long connectReplyId = begin.streamId();
        final long traceId = begin.trace();

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::tryWrap);
        assert httpBeginFW != null;
        final ArrayFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                              getHeader(responseHeaders, ":status"));
        }

        factory.writer.doHttpResponse(
            accept,
            acceptRouteId,
            acceptReplyId,
            traceId,
            builder -> responseHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))));

        // count all responses
        factory.counters.responses.getAsLong();

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
                                  data.trace(),
                                  data.groupId(),
                                  payload.buffer(),
                                  payload.offset(),
                                  payload.sizeof(),
                                  data.reserved());
    }

    private void onEnd(
        final EndFW end)
    {
        final long traceId = end.trace();
        factory.writer.doHttpEnd(accept, acceptRouteId, acceptReplyId, traceId, end.extension());
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        factory.writer.doAbort(accept, acceptRouteId, acceptReplyId, traceId);
    }

    private void onWindow(
        final WindowFW window)
    {
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();
        acceptReplyBudget += credit;
        factory.writer.doWindow(connect,
                                connectRouteId,
                                connectReplyId,
                                window.trace(),
                                credit,
                                padding,
                                groupId);
    }

    private void onReset(
        final ResetFW reset)
    {
        factory.writer.doReset(connect,
                               connectRouteId,
                               connectReplyId,
                               reset.trace());
    }
}
