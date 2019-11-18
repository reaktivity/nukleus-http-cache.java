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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
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

final class HttpCacheProxyNotModifiedResponse
{
    private final HttpCacheProxyFactory factory;

    private final MessageConsumer accept;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private final MessageConsumer connect;
    private final long connectRouteId;
    private final long connectReplyId;

    private final int initialWindow;
    private final int requestHash;
    private final short authScope;
    private final String preferWait;
    private final String requestURL;

    private int connectReplyBudget;

    HttpCacheProxyNotModifiedResponse(
        HttpCacheProxyFactory factory,
        int requestHash,
        short authScope,
        String requestURL,
        MessageConsumer accept,
        long acceptRouteId,
        long acceptReplyId,
        MessageConsumer connect,
        long connectReplyId,
        long connectRouteId,
        String preferWait)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.authScope = authScope;
        this.preferWait = preferWait;
        this.accept = accept;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.connect = connect;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
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
        final OctetsFW extension = factory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        DefaultCacheEntry cacheEntry = factory.defaultCache.supply(requestHash, authScope, requestURL);
        factory.defaultCache.send304(cacheEntry.etag(),
                                     preferWait,
                                     accept,
                                     acceptRouteId,
                                     acceptReplyId);
        sendWindow(initialWindow, begin.traceId());
        factory.defaultCache.updateResponseHeaderIfNecessary(requestHash, responseHeaders);
    }

    private void onData(
        DataFW data)
    {
        sendWindow(data.reserved(), data.traceId());
    }

    private void onEnd(EndFW end)
    {
        factory.writer.doHttpEnd(accept,
                                 acceptRouteId,
                                 acceptReplyId,
                                 end.traceId());

    }

    private void onAbort(AbortFW abort)
    {
        factory.writer.doAbort(accept,
                               acceptRouteId,
                               acceptReplyId,
                               abort.traceId());
    }

    private void onReset(ResetFW reset)
    {
        //NOOP
    }

    private void onWindow(WindowFW window)
    {
        //NOOP
    }

    private void sendWindow(
        int credit,
        long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            factory.writer.doWindow(connect,
                                    connectRouteId,
                                    connectReplyId,
                                    traceId,
                                    0L,
                                    credit,
                                    0);
        }
    }
}
