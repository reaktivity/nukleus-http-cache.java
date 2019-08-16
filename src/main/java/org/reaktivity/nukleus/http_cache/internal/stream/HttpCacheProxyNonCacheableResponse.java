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
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
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

final class HttpCacheProxyNonCacheableResponse extends HttpCacheProxyResponse
{
    private final HttpCacheProxyFactory streamFactory;

    private MessageConsumer streamState;

    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyStreamId;

    private Request streamCorrelation;
    private long acceptInitialId;
    private int acceptReplyBudget;

    private long groupId;
    private int padding;

    private long traceId;

    HttpCacheProxyNonCacheableResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        long acceptInitialId)
    {
        this.streamFactory = httpCacheProxyFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyStreamId = connectReplyId;
        this.acceptInitialId = acceptInitialId;
        this.streamState = this::onResponseMessage;
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
                final BeginFW begin = this.streamFactory.beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
                onWindowWhenProxying(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = streamFactory.resetRO.wrap(buffer, index, index + length);
                onResetWhenProxying(reset);
                break;
            default:
                // ignore
                break;
        }
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, acceptReplyBudget=%016x, " +
                " padding=%d]", getClass().getSimpleName(),
            connectRouteId, connectReplyStreamId, acceptReplyBudget, padding);
    }


    private void handleBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        traceId = begin.trace();

        this.streamCorrelation = this.streamFactory.requestCorrelations.remove(connectReplyId);
        final OctetsFW extension = streamFactory.beginRO.extension();

        if (streamCorrelation != null && extension.sizeof() > 0)
        {
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);

            if (DEBUG)
            {
                System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                        getHeader(httpBeginFW.headers(), ":status"));
            }

            final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
            doProxyBegin(responseHeaders);
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, traceId);
        }
    }

    ///////////// PROXY
    private void doProxyBegin(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyId = streamCorrelation.acceptReplyId();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent proxy response]\n", currentTimeMillis(), acceptReplyId,
                    getHeader(responseHeaders, ":status"));
        }

        streamCorrelation.setThrottle(this::onResponseMessage);
        streamFactory.writer.doHttpResponse(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                traceId,
                builder -> responseHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value()))
            ));

        // count all responses
        streamFactory.counters.responses.getAsLong();

        this.streamState = this::onStreamMessageWhenProxying;
    }

    private void onStreamMessageWhenProxying(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
            onDataWhenProxying(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
            onEndWhenProxying(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = streamFactory.abortRO.wrap(buffer, index, index + length);
            onAbortWhenProxying(abort);
            break;
        default:
            streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                                         streamFactory.supplyTrace.getAsLong());
            break;
        }
    }

    private void onDataWhenProxying(
        final DataFW data)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        final OctetsFW payload = data.payload();
        acceptReplyBudget -= payload.sizeof() + data.padding();
        assert acceptReplyBudget >= 0;
        streamFactory.writer.doHttpData(
            acceptReply,
            acceptRouteId,
            acceptReplyStreamId,
            data.trace(),
            data.groupId(),
            payload.buffer(),
            payload.offset(),
            payload.sizeof(),
            data.padding()
                                       );
    }

    private void onEndWhenProxying(
        final EndFW end)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();
        final long traceId = end.trace();
        streamFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyStreamId, traceId, end.extension());
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }

    private void onAbortWhenProxying(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        streamFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyStreamId, traceId);
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }

    private void onWindowWhenProxying(
        final WindowFW window)
    {
        final int credit = window.credit();
        padding = window.padding();
        groupId = window.groupId();
        acceptReplyBudget +=credit;
        streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                                      connectReplyStreamId, window.trace(), credit, padding, groupId);
    }

    private void onResetWhenProxying(
        final ResetFW reset)
    {
        streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, reset.trace());
        streamCorrelation.purge();
        streamFactory.cleanupCorrelationIfNecessary(connectReplyStreamId, acceptInitialId);
    }
}
