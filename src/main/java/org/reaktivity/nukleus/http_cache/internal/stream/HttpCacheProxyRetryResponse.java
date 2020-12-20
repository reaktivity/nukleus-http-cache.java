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

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

final class HttpCacheProxyRetryResponse
{
    private final HttpCacheProxyFactory factory;
    private final int requestHash;
    private final MessageConsumer initial;
    private final long routeId;
    private final long replyId;
    private final LongConsumer retryRequestAfter;

    private long replySeq;
    private long replyAck;
    private int replyMax;

    private long retryAfter;

    HttpCacheProxyRetryResponse(
        HttpCacheProxyFactory factory,
        int requestHash,
        MessageConsumer initial,
        long routeId,
        long initialId,
        LongConsumer retryRequestAfter)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.initial = initial;
        this.routeId = routeId;
        this.replyId = factory.supplyReplyId.applyAsLong(initialId);
        this.retryRequestAfter = retryRequestAfter;
    }

    @Override
    public String toString()
    {
        return String.format("%s[routeId=%016x, replyId=%d]", getClass().getSimpleName(), routeId, replyId);
    }

    void doResponseReset(
        long traceId)
    {
        factory.writer.doReset(initial, routeId, replyId, 0L, 0L, 0, traceId);
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
        }
    }

    private void onResponseBegin(
        BeginFW begin)
    {
        final long traceId = begin.traceId();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final Array32FW<HttpHeaderFW> headers = httpBeginFW.headers();
        final String status = getHeader(headers, STATUS);
        assert status != null;

        retryAfter = HttpHeadersUtil.retryAfter(headers);

        factory.defaultCache.updateResponseHeaderIfNecessary(requestHash, headers);

        doResponseWindow(traceId, 0, factory.initialWindowSize);
    }

    private void onResponseData(
        DataFW data)
    {
        final long sequence = data.sequence();
        final long acknowledge = data.acknowledge();
        final long traceId = data.traceId();
        final int reserved = data.reserved();

        assert acknowledge <= sequence;
        assert sequence >= replySeq;

        replySeq = sequence + reserved;

        assert replyAck <= replySeq;

        doResponseWindow(traceId, 0, replyMax);
    }

    private void onResponseEnd(
        EndFW end)
    {
        retryRequestAfter.accept(retryAfter);
    }

    private void onResponseAbort(
        AbortFW abort)
    {
        retryRequestAfter.accept(retryAfter);
    }

    private void doResponseWindow(
        long traceId,
        int minReplyNoAck,
        int minReplyMax)
    {
        final long newReplyAck = Math.max(replySeq - minReplyNoAck, replyAck);

        if (newReplyAck > replyAck || minReplyMax > replyMax)
        {
            replyAck = newReplyAck;
            assert replyAck <= replySeq;

            replyMax = minReplyMax;

            factory.writer.doWindow(initial,
                                    routeId,
                                    replyId,
                                    replySeq,
                                    replyAck,
                                    replyMax,
                                    traceId,
                                    0L,
                                    0);
        }
    }
}
