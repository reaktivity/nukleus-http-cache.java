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
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Function;

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

final class HttpCacheProxyCacheableResponse
{
    private final HttpCacheProxyFactory factory;

    private int connectReplyBudget;
    private long traceId;
    private boolean isResponseBuffering;
    private int requestHash;

    private int requestSlot;
    private final int initialWindow;
    private String ifNoneMatch;
    private final Function<Long, Boolean> retryRequest;
    private final HttpProxyCacheableRequestGroup requestGroup;

    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptReplyId;

    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private DefaultCacheEntry cacheEntry;

    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        int requestHash,
        int requestSlot,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        MessageConsumer connectReply,
        long connectReplyId,
        long connectRouteId,
        String ifNoneMatch,
        Function<Long, Boolean> retryRequest)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.requestSlot = requestSlot;
        this.requestHash = requestHash;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
        this.ifNoneMatch = ifNoneMatch;
        this.retryRequest = retryRequest;
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectRouteId=%016x, connectReplyStreamId=%d, " +
                "connectReplyBudget=%d]", getClass().getSimpleName(), connectRouteId, connectReplyId, connectReplyBudget);
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
        traceId = begin.trace();

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [received response]\n", currentTimeMillis(), connectReplyId,
                    getHeader(httpBeginFW.headers(), ":status"));
        }

        final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

        cacheEntry = factory.defaultCache.supply(requestHash);
        cacheEntry.setSubscribers(requestGroup.getNumberOfRequests());
        String etag = getHeader(responseHeaders, ETAG);
        isResponseBuffering = etag == null;

        //Initial cache entry
        if (cacheEntry.etag() == null && cacheEntry.requestHeadersSize() == 0)
        {
            if (!cacheEntry.storeRequestHeaders(getRequestHeaders()) ||
                !cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //This should never happen.
                assert false;
            }
        }
        else
        {
            cacheEntry.evictResponse();
            if (!cacheEntry.storeResponseHeaders(responseHeaders))
            {
                //This should never happen.
                assert false;
            }
        }
        cacheEntry.setEtag(etag);
        if (!isResponseBuffering)
        {
            requestGroup.onCacheableResponseUpdated();
        }

        sendWindow(initialWindow, traceId);
    }

    private void onData(
        DataFW data)
    {
        boolean stored = cacheEntry.storeResponseData(data);
        if (!stored)
        {
            //This should never happen.
            assert false;
        }
        sendWindow(data.length() + data.padding(), data.trace());
        if (!isResponseBuffering)
        {
            requestGroup.onCacheableResponseUpdated();
        }
    }

    private void onEnd(
        EndFW end)
    {
        checkEtag(end, cacheEntry);
        cacheEntry.setResponseCompleted(true);

        if (!factory.defaultCache.isUpdatedByEtagToRetry(getRequestHeaders(),
                                                         ifNoneMatch,
                                                         cacheEntry))
        {
            long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
            retryRequest.apply(retryAfter);
        }
        else
        {
            requestGroup.onCacheableResponseUpdated();
        }
    }

    private void onAbort(AbortFW abort)
    {
        if (isResponseBuffering)
        {
            factory.counters.responses.getAsLong();
            factory.writer.do503AndAbort(acceptReply,
                                         acceptRouteId,
                                         acceptReplyId,
                                         abort.trace());
            requestGroup.onNonCacheableResponse(acceptReplyId);
        }
        else
        {
            requestGroup.onCacheableResponseAborted();
        }

        purgeRequest();
        factory.defaultCache.purge(requestHash);
    }

    private void checkEtag(
        EndFW end,
        DefaultCacheEntry cacheEntry)
    {
        final OctetsFW extension = end.extension();
        if (extension.sizeof() > 0)
        {
            final HttpEndExFW httpEndEx = extension.get(factory.httpEndExRO::wrap);
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
            factory.writer.doWindow(connectReply,
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
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    private void purgeRequest()
    {
        if (requestSlot != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot);
            this.requestSlot = NO_SLOT;
        }
    }
}
