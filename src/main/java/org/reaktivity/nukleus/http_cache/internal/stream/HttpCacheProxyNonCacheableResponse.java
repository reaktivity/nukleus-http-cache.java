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
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

final class HttpCacheProxyNonCacheableResponse
{
    private final HttpCacheProxyFactory httpCacheProxyFactory;

    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyId;

    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private int acceptReplyBudget;
    private long groupId;
    private int padding;

    HttpCacheProxyNonCacheableResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId)
    {
        this.httpCacheProxyFactory = httpCacheProxyFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, padding=%d]",
                             getClass().getSimpleName(), connectRouteId, connectReplyId,  padding);
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
            case ResetFW.TYPE_ID:
                final ResetFW reset = httpCacheProxyFactory.resetRO.wrap(buffer, index, index + length);
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
        final HttpBeginExFW httpBeginFW = extension.get(httpCacheProxyFactory.httpBeginExRO::tryWrap);
        assert httpBeginFW != null;
        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                              getHeader(responseHeaders, ":status"));
        }

        httpCacheProxyFactory.writer.doHttpResponse(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            traceId,
            builder -> responseHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))));

        // count all responses
        httpCacheProxyFactory.counters.responses.getAsLong();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent proxy response]\n", currentTimeMillis(), acceptReplyId,
                              getHeader(responseHeaders, ":status"));
        }
    }

    private void onData(
        final DataFW data)
    {
        final OctetsFW payload = data.payload();
        acceptReplyBudget -= payload.sizeof() + data.padding();
        assert acceptReplyBudget >= 0;
        httpCacheProxyFactory.writer.doHttpData(acceptReply,
                                                acceptRouteId,
                                                acceptReplyId,
                                                data.trace(),
                                                data.groupId(),
                                                payload.buffer(),
                                                payload.offset(),
                                                payload.sizeof(),
                                                data.padding());
    }

    private void onEnd(
        final EndFW end)
    {
        final long traceId = end.trace();
        httpCacheProxyFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, traceId, end.extension());
    }

    private void onAbort(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        httpCacheProxyFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, traceId);
    }

    private void onWindow(
        final WindowFW window)
    {
        final int credit = window.credit();
        padding = window.padding();
        groupId = window.groupId();
        acceptReplyBudget += credit;
        httpCacheProxyFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                                              connectReplyId, window.trace(), credit, padding, groupId);
    }

    private void onReset(
        final ResetFW reset)
    {
        httpCacheProxyFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, reset.trace());
    }
}
