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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
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
    private boolean isResponseBuffering;
    private int requestHash;

    private final MutableInteger requestSlot;
    private final String requestURL;
    private final int initialWindow;
    private String ifNoneMatch;
    private final Function<Long, Boolean> retryRequest;
    private final HttpProxyCacheableRequestGroup requestGroup;

    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private DefaultCacheEntry cacheEntry;
    private String etag;

    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
<<<<<<< HEAD
        int requestHash,
        String requestURL,
=======
>>>>>>> cb82714e1d2831ed9f4dd830a5145b2822faabf1
        MutableInteger requestSlot,
        MessageConsumer connectReply,
        long connectReplyId,
        long connectRouteId,
        Function<Long, Boolean> retryRequest)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.requestURL = requestURL;
        this.requestSlot = requestSlot;
        this.requestHash = requestGroup.getRequestHash();
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
        this.ifNoneMatch = requestGroup.getEtag();
        this.retryRequest = retryRequest;
        assert requestSlot.value != NO_SLOT;
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
        long traceId = begin.traceId();

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);


        final ArrayFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

<<<<<<< HEAD
        cacheEntry = factory.defaultCache.supply(requestHash, requestURL);
        cacheEntry.setSubscribers(requestGroup.getNumberOfRequests());
        String etag = getHeader(responseHeaders, ETAG);
=======
        cacheEntry = factory.defaultCache.supply(requestHash);
        cacheEntry.setSubscribers(requestGroup.getQueuedRequests());
        etag = getHeader(responseHeaders, ETAG);
>>>>>>> cb82714e1d2831ed9f4dd830a5145b2822faabf1
        isResponseBuffering = etag == null;

        if (!cacheEntry.storeRequestHeaders(getRequestHeaders()) ||
            !cacheEntry.storeResponseHeaders(responseHeaders))
        {
            //This should never happen.
            assert false;
        }

        cacheEntry.setEtag(etag);
        if (!isResponseBuffering)
        {
            requestGroup.onCacheableResponseUpdated(etag);
        }

        sendWindow(initialWindow, traceId);
    }

    private void onData(
        DataFW data)
    {
        sendWindow(data.reserved(), data.traceId());
        boolean stored = cacheEntry.storeResponseData(data);
        assert stored;
        if (!isResponseBuffering)
        {
            requestGroup.onCacheableResponseUpdated(etag);
        }
    }

    private void onEnd(
        EndFW end)
    {
        checkEtag(end, cacheEntry);
        cacheEntry.setResponseCompleted(true);

        if (isResponseBuffering &&
            factory.defaultCache.checkTrailerToRetry(ifNoneMatch,
                                                     cacheEntry))
        {
            long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
            retryRequest.apply(retryAfter);
        }
        else
        {
            requestGroup.onCacheableResponseUpdated(etag);
        }
    }

    private void onAbort(AbortFW abort)
    {
        assert requestSlot.value != NO_SLOT;
        requestGroup.onCacheableResponseAborted();

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
            ArrayFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> ETAG.equals(h.name().asString()));
            if (etag != null)
            {
                this.etag = etag.value().asString();
                cacheEntry.setEtag(this.etag);
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
                                    0L,
                                    credit,
                                    0);
        }
    }

    private ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot.value);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    private void purgeRequest()
    {
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }
    }
}
