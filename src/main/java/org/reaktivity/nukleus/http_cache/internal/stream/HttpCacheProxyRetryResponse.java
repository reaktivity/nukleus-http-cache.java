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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

import java.util.function.Function;

final class HttpCacheProxyRetryResponse
{
    private final HttpCacheProxyFactory factory;

    private final int initialWindow;
    private int connectReplyBudget;
    private long retryAfter;

    private final int requestHash;
    private final MessageConsumer connectReply;
    private final long connectRouteId;
    private final long connectReplyId;
    private Function<Long, Boolean> scheduleRequest;


    HttpCacheProxyRetryResponse(
        HttpCacheProxyFactory factory,
        int requestHash,
        MessageConsumer connectReply,
        long connectRouteId,
        long connectReplyId,
        Function<Long, Boolean> scheduleRequest)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.scheduleRequest = scheduleRequest;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
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
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                              getHeader(httpBeginFW.headers(), ":status"));
        }

        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
        String status = getHeader(responseHeaders, STATUS);
        retryAfter = HttpHeadersUtil.retryAfter(responseHeaders);
        assert status != null;

        factory.defaultCache.updateResponseHeaderIfNecessary(requestHash, responseHeaders);

        sendWindow(initialWindow, begin.trace());
    }

    private void onData(
        DataFW data)
    {
        sendWindow(data.length() + data.padding(), data.trace());
    }

    private void onEnd(EndFW end)
    {
        scheduleRequest.apply(retryAfter);
    }

    private void onAbort(AbortFW abort)
    {
        scheduleRequest.apply(retryAfter);
    }

    private void sendWindow(
        int credit,
        long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            factory.writer.doWindow(connectReply,
                                    connectRouteId,
                                    connectReplyId,
                                    traceId,
                                    credit,
                                    0,
                                    0L);
        }
    }

}