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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

final class HttpCacheProxyNotModifiedResponse
{
    private final HttpCacheProxyFactory httpCacheProxyFactory;
    private final HttpCacheProxyCacheableRequest request;

    private final int initialWindow;
    private int connectReplyBudget;


    HttpCacheProxyNotModifiedResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        HttpCacheProxyCacheableRequest request)
    {
        this.httpCacheProxyFactory = httpCacheProxyFactory;
        this.request = request;
        this.initialWindow = httpCacheProxyFactory.responseBufferPool.slotCapacity();
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d]",
                             getClass().getSimpleName(),
                             request.connectRouteId,
                             request.connectReplyId);
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
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        final OctetsFW extension = httpCacheProxyFactory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(httpCacheProxyFactory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                              getHeader(responseHeaders, ":status"));
        }
        DefaultCacheEntry cacheEntry = httpCacheProxyFactory.defaultCache.supply(request.requestHash());
        httpCacheProxyFactory.defaultCache.send304(cacheEntry, request);
        sendWindow(initialWindow, begin.trace());
    }

    private void onData(
        DataFW data)
    {
        sendWindow(data.length() + data.padding(), data.trace());
    }

    private void onEnd(EndFW end)
    {
        //NOOP
    }

    private void onAbort(AbortFW abort)
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
            httpCacheProxyFactory.writer.doWindow(request.connectReply,
                                                  request.connectRouteId,
                                                  request.connectReplyId,
                                                  traceId,
                                                  credit,
                                                  0,
                                                  0L);
        }
    }

}