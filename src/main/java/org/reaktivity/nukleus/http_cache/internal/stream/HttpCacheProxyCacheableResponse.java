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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpEndExFW;

import java.util.function.Function;

final class HttpCacheProxyCacheableResponse
{
    private final HttpCacheProxyFactory httpCacheProxyFactory;

    private int connectReplyBudget;
    private long traceId;
    private boolean isResponseBuffering;
    private int requestHash;

    private final int requestSlot;
    private final int initialWindow;
    private String ifNoneMatch;
    private final Function<Long, Boolean> retryRequest;
    private final HttpProxyCacheableRequestGroup requestGroup;

    public final MessageConsumer acceptReply;
    public final long acceptRouteId;
    public final long acceptInitialId;
    public final long acceptReplyId;

    public MessageConsumer connectReply;
    public long connectRouteId;
    public long connectReplyId;

    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory httpCacheProxyFactory,
        HttpProxyCacheableRequestGroup requestGroup,
        int requestHash,
        int requestSlot,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptInitialId,
        long acceptReplyId,
        MessageConsumer connectReply,
        long connectReplyId,
        long connectRouteId,
        String ifNoneMatch,
        Function<Long, Boolean> retryRequest)
    {
        this.httpCacheProxyFactory = httpCacheProxyFactory;
        this.requestGroup = requestGroup;
        this.requestSlot = requestSlot;
        this.requestHash = requestHash;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.initialWindow = httpCacheProxyFactory.responseBufferPool.slotCapacity();
        this.ifNoneMatch = ifNoneMatch;
        this.retryRequest = retryRequest;
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, " +
                "connectReplyBudget=%d]", getClass().getSimpleName(),
            connectRouteId, connectReplyId, connectReplyBudget);
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
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
        traceId = begin.trace();

        final OctetsFW extension = httpCacheProxyFactory.beginRO.extension();
        final HttpBeginExFW httpBeginFW = extension.get(httpCacheProxyFactory.httpBeginExRO::wrap);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                    getHeader(httpBeginFW.headers(), ":status"));
        }

        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        DefaultCacheEntry cacheEntry = httpCacheProxyFactory.defaultCache.supply(requestHash);
        isResponseBuffering = getHeader(responseHeaders, ETAG) != null;

        //Initial cache entry
        if(cacheEntry.etag() == null && cacheEntry.requestHeadersSize() == 0)
        {
            if (!cacheEntry.storeRequestHeaders(getRequestHeaders())
                || !cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //TODO: Better handle if there is no slot available, For example, release response payload
                // which requests are in flight
//                request.purge();
            }
            if(!isResponseBuffering)
            {
                requestGroup.signalForUpdatedCacheEntry(requestHash);
            }

        }
        else
        {
            cacheEntry.evictResponse();
            if (!cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //TODO: Better handle if there is no slot available, For example, release response payload
                // which requests are in flight
//                request.purge();
            }
            if(!isResponseBuffering)
            {
                requestGroup.signalForUpdatedCacheEntry(requestHash);
            }
        }

        sendWindow(initialWindow, traceId);
    }

    private void onData(
        DataFW data)
    {
        DefaultCacheEntry cacheEntry = httpCacheProxyFactory.defaultCache.get(requestHash);
        boolean stored = cacheEntry.storeResponseData(data);
        if (!stored)
        {
            //TODO: Better handle if there is no slot available, For example, release response payload
            // which requests are in flight
//            request.purge();
        }
        if (!isResponseBuffering)
        {
            requestGroup.signalForUpdatedCacheEntry(requestHash);
        }
        sendWindow(data.length() + data.padding(), data.trace());
    }

    private void onEnd(
        EndFW end)
    {
        DefaultCacheEntry cacheEntry = this.httpCacheProxyFactory.defaultCache.get(requestHash);

        checkEtag(end, cacheEntry);
        cacheEntry.setResponseCompleted(true);

        if (!httpCacheProxyFactory.defaultCache.isUpdatedByEtagToRetry(getRequestHeaders(),
                                                                       ifNoneMatch,
                                                                       cacheEntry))
        {
            long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
            retryRequest.apply(retryAfter);
        }
        else
        {
            requestGroup.signalForUpdatedCacheEntry(requestHash);
        }
    }

    private void onAbort(AbortFW abort)
    {
        if (isResponseBuffering)
        {
            httpCacheProxyFactory.writer.do503AndAbort(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       abort.trace(),
                                                       0L);
        }
        else
        {
            requestGroup.signalAbortAllSubscribers(requestHash);
        }
        httpCacheProxyFactory.defaultCache.purge(requestHash);
    }

    private void checkEtag(
        EndFW end,
        DefaultCacheEntry cacheEntry)
    {
        final OctetsFW extension = end.extension();
        if (extension.sizeof() > 0)
        {
            final HttpEndExFW httpEndEx = extension.get(httpCacheProxyFactory.httpEndExRO::wrap);
            ListFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> ETAG.equals(h.name().asString()));
            if (etag != null)
            {
                cacheEntry.setEtag(etag.value().asString());
                isResponseBuffering = false;
            }
        }
    }

    private void sendWindow(
        int credit,
        long traceId)
    {
        connectReplyBudget += credit;
        if (connectReplyBudget > 0)
        {
            httpCacheProxyFactory.writer.doWindow(connectReply,
                                                  connectRouteId,
                                                  connectReplyId,
                                                  traceId,
                                                  credit,
                                                  0,
                                                  0L);
        }
    }

    private ListFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = httpCacheProxyFactory.requestBufferPool.buffer(requestSlot);
        return httpCacheProxyFactory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }
}
