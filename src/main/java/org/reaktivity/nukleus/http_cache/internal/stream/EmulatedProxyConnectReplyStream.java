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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.CacheRefreshRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager.StreamKind;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
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
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;


final class EmulatedProxyConnectReplyStream
{
    private final HttpCacheProxyFactory streamFactory;

    private MessageConsumer streamState;

    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyStreamId;

    private Request streamCorrelation;

    private int acceptReplyBudget;
    private int connectReplyBudget;
    private long groupId;
    private int padding;
    private boolean endDeferred;
    private boolean cached;
    private OctetsFW endExtension;

    EmulatedProxyConnectReplyStream(
        HttpCacheProxyFactory httpCacheProxyFactory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId)
    {
        this.streamFactory = httpCacheProxyFactory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyStreamId = connectReplyId;
        this.streamState = this::beforeBegin;
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
                "connectReplyBudget=%d, padding=%d]", getClass().getSimpleName(),
            connectRouteId, connectReplyStreamId, acceptReplyBudget, connectReplyBudget, padding);
    }

    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = this.streamFactory.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                    streamFactory.supplyTrace.getAsLong());
        }
    }

    private void handleBegin(
        BeginFW begin)
    {
        final long connectReplyId = begin.streamId();
        final long traceId = begin.trace();

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

            switch(streamCorrelation.getType())
            {
                case PROXY:
                    doProxyBegin(traceId, responseHeaders);
                    break;
                case INITIAL_REQUEST:
                    handleInitialRequest(traceId, responseHeaders);
                    break;
                case CACHE_REFRESH:
                    handleCacheRefresh(traceId, responseHeaders);
                    break;
                default:
                    throw new RuntimeException("Not implemented");
            }
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, 0L);
        }
    }

    ///////////// CACHE REFRESH
    private void handleCacheRefresh(
        long traceId, ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        if (retry && ((CacheableRequest)streamCorrelation).attempts() < 3)
        {
            retryCacheableRequest(traceId);
            return;
        }
        CacheRefreshRequest request = (CacheRefreshRequest) this.streamCorrelation;
        if (request.storeResponseHeaders(responseHeaders, streamFactory.emulatedCache, streamFactory.responseBufferPool))
        {
            this.streamState = this::handleCacheRefresh;
            streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                streamFactory.supplyTrace.getAsLong(), 32767, 0, 0L);
        }
        else
        {
            request.purge();
            this.streamState = this::reset;
            streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                    streamFactory.supplyTrace.getAsLong());
        }
    }

    private void reset(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // NOOP
    }

    private void handleCacheRefresh(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                boolean stored = request.storeResponseData(data, streamFactory.responseBufferPool);
                if (!stored)
                {
                    request.purge();
                    this.streamState = this::reset;
                    streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                            streamFactory.supplyTrace.getAsLong());
                }
                else
                {
                    streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                        streamFactory.supplyTrace.getAsLong(), length, 0, 0L);
                }
                break;
            case EndFW.TYPE_ID:
                final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
                checkEtag(end, request);
                cached = request.cache(end, streamFactory.emulatedCache);
                break;
            case AbortFW.TYPE_ID:
            default:
                request.purge();
                break;
        }
    }

    ///////////// INITIAL_REQUEST REQUEST
    private void handleInitialRequest(
        Long traceId, ListFW<HttpHeaderFW> responseHeaders)
    {
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        if (retry && ((CacheableRequest)streamCorrelation).attempts() < 3)
        {
            retryCacheableRequest(traceId);
            return;
        }
        int freshnessExtension = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        final boolean isCacheableResponse = isCacheableResponse(responseHeaders);

        if (freshnessExtension > 0 && isCacheableResponse)
        {
            handleEdgeArchSync(responseHeaders, freshnessExtension, traceId);
        }
        else if(isCacheableResponse)
        {
            handleCacheableResponse(responseHeaders, traceId);
        }
        else
        {
            streamCorrelation.purge();
            doProxyBegin(traceId, responseHeaders);
        }
    }

    private void handleEdgeArchSync(
        ListFW<HttpHeaderFW> responseHeaders,
        int freshnessExtension,
        long traceId)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        if (request.storeResponseHeaders(responseHeaders, streamFactory.emulatedCache, streamFactory.responseBufferPool))
        {
            final MessageConsumer acceptReply = streamCorrelation.acceptReply();
            final long acceptRouteId = streamCorrelation.acceptRouteId();
            final long acceptReplyId = streamCorrelation.acceptReplyId();

            if (DEBUG)
            {
                System.out.printf("[%016x] ACCEPT %016x %s [sent cacheable response] [%s]\n", currentTimeMillis(), acceptReplyId,
                        getHeader(responseHeaders, ":status"), this.toString());
            }

            streamCorrelation.setThrottle(this::onThrottleMessageWhenProxying);
            streamFactory.writer.doHttpResponseWithUpdatedCacheControl(
                    acceptReply,
                    acceptRouteId,
                    acceptReplyId,
                    streamFactory.cacheControlParser,
                    responseHeaders,
                    freshnessExtension,
                    request.etag(),
                    false,
                    traceId
                    );

            // count all responses
            streamFactory.counters.responses.getAsLong();

            // count all promises (prefer wait, if-none-match)
            streamFactory.counters.promises.getAsLong();

            this.streamState = this::handleCacheableRequestResponse;
        }
        else
        {
            request.purge();
            doProxyBegin(traceId, responseHeaders);
        }
    }

    private void retryCacheableRequest(long traceId)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;
        request.incAttempts();

        long connectInitialId = request.supplyInitialId().applyAsLong(connectRouteId);
        MessageConsumer connectInitial = this.streamFactory.router.supplyReceiver(connectInitialId);
        long connectReplyId = request.supplyReplyId().applyAsLong(connectInitialId);

        streamFactory.requestCorrelations.put(connectReplyId, request);
        ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(streamFactory.requestHeadersRO);
        final String etag = request.etag();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                    currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        streamFactory.writer.doHttpRequest(connectInitial, connectRouteId, connectInitialId, traceId, builder ->
        {
            requestHeaders.forEach(
                    h ->  builder.item(item -> item.name(h.name()).value(h.value())));
            if (request instanceof CacheRefreshRequest)
            {
                builder.item(item -> item.name(HttpHeaders.IF_NONE_MATCH).value(etag));
            }
        });
        streamFactory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, streamFactory.supplyTrace.getAsLong());
        streamFactory.counters.requestsRetry.getAsLong();
    }

    private void handleCacheableResponse(
        ListFW<HttpHeaderFW> responseHeaders,
        long traceId)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;
        if (!request.storeResponseHeaders(responseHeaders, streamFactory.emulatedCache, streamFactory.responseBufferPool))
        {
            request.purge();
        }
        doProxyBegin(traceId, responseHeaders);
        this.streamState = this::handleCacheableRequestResponse;
    }

    private void handleCacheableRequestResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;

        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
            boolean stored = request.storeResponseData(data, streamFactory.responseBufferPool);
            if (!stored)
            {
                request.purge();
            }
            break;
        case EndFW.TYPE_ID:
            final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
            checkEtag(end, request);
            cached = request.cache(end, streamFactory.emulatedCache);
            break;
        case AbortFW.TYPE_ID:
        default:
            request.purge();
            break;
        }
        this.onStreamMessageWhenProxying(msgTypeId, buffer, index, length);
    }

    ///////////// PROXY
    private void doProxyBegin(
        long traceId, ListFW<HttpHeaderFW> responseHeaders)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyId = streamCorrelation.acceptReplyId();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent proxy response]\n", currentTimeMillis(), acceptReplyId,
                    getHeader(responseHeaders, ":status"));
        }

        streamCorrelation.setThrottle(this::onThrottleMessageWhenProxying);
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

    private void onThrottleMessageWhenProxying(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
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

    private void onDataWhenProxying(
        final DataFW data)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        connectReplyBudget -= data.length() + data.padding();
        if (connectReplyBudget < 0)
        {
            streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId,
                    streamFactory.supplyTrace.getAsLong());
        }
        else
        {
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
    }

    private void onEndWhenProxying(
        final EndFW end)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();
        final long traceId = end.trace();
        streamFactory.budgetManager.closing(groupId, acceptReplyStreamId, connectReplyBudget, traceId);
        if (streamFactory.budgetManager.hasUnackedBudget(groupId, acceptReplyStreamId))
        {
            endDeferred = true;
        }
        else
        {
            streamFactory.budgetManager.closed(StreamKind.PROXY, groupId, acceptReplyStreamId, traceId);
            streamFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyStreamId, traceId, end.extension());
        }
    }

    private void onAbortWhenProxying(
        final AbortFW abort)
    {
        final long traceId = abort.trace();
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptRouteId = streamCorrelation.acceptRouteId();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();

        streamFactory.budgetManager.closed(StreamKind.PROXY, groupId, acceptReplyStreamId, traceId);
        streamFactory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyStreamId, traceId);
    }

    private void onWindowWhenProxying(
        final WindowFW window)
    {
        final long streamId = window.streamId();
        final int credit = window.credit();
        acceptReplyBudget += credit;
        padding = window.padding();
        groupId = window.groupId();
        streamFactory.budgetManager.window(StreamKind.PROXY, groupId, streamId, credit,
            this::budgetAvailableWhenProxying, window.trace());
        if (endDeferred && !streamFactory.budgetManager.hasUnackedBudget(groupId, streamId))
        {
            final long acceptRouteId = streamCorrelation.acceptRouteId();
            final long acceptReplyStreamId = streamCorrelation.acceptReplyId();
            final MessageConsumer acceptReply = streamCorrelation.acceptReply();
            streamFactory.budgetManager.closed(StreamKind.PROXY, groupId, acceptReplyStreamId, window.trace());
            if (this.endExtension != null && this.endExtension.sizeof() > 0)
            {
                streamFactory.writer.doHttpEnd(acceptReply,
                                                acceptRouteId,
                                                acceptReplyStreamId,
                                                window.trace(),
                                                this.endExtension);
            }
            else
            {
                streamFactory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyStreamId, window.trace());
            }

        }
    }

    private void onResetWhenProxying(
        final ResetFW reset)
    {
        final long acceptReplyStreamId = streamCorrelation.acceptReplyId();
        streamFactory.budgetManager.closed(StreamKind.PROXY, groupId, acceptReplyStreamId, reset.trace());
        streamFactory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, reset.trace());
        // if cached, do not purge the buffer slots as it may be used by other clients
        if (!cached)
        {
            streamCorrelation.purge();
        }
    }

    private int budgetAvailableWhenProxying(
        int credit,
        long trace)
    {
        if (endDeferred)
        {
            return credit;
        }
        else
        {
            connectReplyBudget += credit;
            streamFactory.writer.doWindow(connectReplyThrottle, connectRouteId,
                                          connectReplyStreamId, trace, credit, padding, groupId);

            return 0;
        }
    }

    private void checkEtag(EndFW end, CacheableRequest request)
    {
        final OctetsFW extension = end.extension();
        if (extension.sizeof() > 0)
        {
            final HttpEndExFW httpEndEx = extension.get(streamFactory.httpEndExRO::wrap);
            ListFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> "etag".equals(h.name().asString()));
            if (etag != null)
            {
                request.etag(etag.value().asString());
            }
            this.endExtension = extension;
        }
    }
}
