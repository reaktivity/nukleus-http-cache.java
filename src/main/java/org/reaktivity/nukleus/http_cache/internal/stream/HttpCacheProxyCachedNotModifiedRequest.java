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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

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

final class HttpCacheProxyCachedNotModifiedRequest
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptReplyId;
    private final long acceptInitialId;
    private final int initialWindow;

    HttpCacheProxyCachedNotModifiedRequest(
        HttpCacheProxyFactory factory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        long acceptInitialId)
    {
        this.factory = factory;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.acceptInitialId = acceptInitialId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
    }

    void onAccept(
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
        case ResetFW.TYPE_ID:
            onReset();
            break;
        default:
            break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

        // count all requests
        factory.counters.requests.getAsLong();
        factory.counters.requestsCacheable.getAsLong();

        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                begin.trace(),
                                initialWindow,
                                0,
                                0L);


        factory.writer.do304(acceptReply,
                             acceptRouteId,
                             acceptReplyId,
                             factory.supplyTrace.getAsLong(),
                             requestHeaders);
    }

    private void onData(
        final DataFW data)
    {
        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                data.trace(),
                                data.reserved(),
                                0,
                                data.groupId());
    }

    private void onEnd(
        final EndFW end)
    {
        factory.writer.doHttpEnd(acceptReply,
                                 acceptRouteId,
                                 acceptReplyId,
                                 factory.supplyTrace.getAsLong());
    }

    private void onAbort(
        final AbortFW abort)
    {
        factory.writer.doAbort(acceptReply,
                               acceptRouteId,
                               acceptReplyId,
                               factory.supplyTrace.getAsLong());
    }

    private void onReset()
    {
        factory.writer.doReset(acceptReply,
                               acceptRouteId,
                               acceptInitialId,
                               factory.supplyTrace.getAsLong());
    }
}
