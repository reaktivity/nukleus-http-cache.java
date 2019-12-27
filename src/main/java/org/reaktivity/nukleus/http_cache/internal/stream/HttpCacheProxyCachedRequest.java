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

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;

import java.time.Instant;

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

final class HttpCacheProxyCachedRequest
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer reply;
    private final long routeId;
    private final long initialId;
    private final int requestHash;
    private final int initialWindow;

    private long authorization;
    private DefaultCacheEntry cacheEntry;
    private boolean promiseNextPollRequest;

    HttpCacheProxyCachedRequest(
        HttpCacheProxyFactory factory,
        int requestHash,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptInitialId)
    {
        this.factory = factory;
        this.requestHash = requestHash;
        this.reply = acceptReply;
        this.routeId = acceptRouteId;
        this.initialId = acceptInitialId;
        this.initialWindow = factory.initialWindowSize;
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
        }
    }

    private void onRequestBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();
        final long traceId = begin.traceId();
        authorization = begin.authorization();
        promiseNextPollRequest = requestHeaders.anyMatch(HAS_EMULATED_PROTOCOL_STACK);
        cacheEntry = factory.defaultCache.get(requestHash);
        cacheEntry.setSubscribers(1);

        factory.writer.doWindow(reply,
                                routeId,
                                initialId,
                                traceId,
                                0L,
                                initialWindow,
                                0);
    }

    private void onRequestData(
        final DataFW data)
    {
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int reserved = data.reserved();
        final int padding = 0;

        doRequestWindow(traceId, budgetId, reserved, padding);
    }

    private void onRequestEnd(
        final EndFW end)
    {
        final long traceId = end.traceId();

        final long replyId = factory.supplyReplyId.applyAsLong(initialId);
        final HttpCacheProxyCachedResponse response = new HttpCacheProxyCachedResponse(
                factory, reply, routeId, replyId, authorization,
                cacheEntry, promiseNextPollRequest, r -> {});
        final Instant now = Instant.now();
        response.doResponseBegin(now, traceId);
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        // nop, response not started
    }

    private void doRequestWindow(
        long traceId,
        long budgetId,
        int reserved,
        int padding)
    {
        factory.writer.doWindow(reply,
                                routeId,
                                initialId,
                                traceId,
                                budgetId,
                                reserved,
                                padding);
    }
}
