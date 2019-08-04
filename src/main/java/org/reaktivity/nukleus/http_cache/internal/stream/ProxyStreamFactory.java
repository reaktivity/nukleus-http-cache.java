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

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;

import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.emulated.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpEndExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();
    final SignalFW signalRO = new SignalFW();

    final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    final HttpEndExFW httpEndExRO = new HttpEndExFW();
    final HttpEndExFW.Builder httpEndExRW = new HttpEndExFW.Builder();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    final RouteManager router;
    final BudgetManager budgetManager;

    final LongUnaryOperator supplyInitialId;
    final LongUnaryOperator supplyReplyId;
    final LongSupplier supplyTrace;
    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;
    public final Long2ObjectHashMap<Request> requestCorrelations;
    final Long2ObjectHashMap<ProxyConnectReplyStream> correlations;
    final Long2ObjectHashMap<Future<?>> expiryRequestsCorrelations;

    final Writer writer;
    final CacheControl cacheControlParser = new CacheControl();
    final Cache emulatedCache;
    final DefaultCache defaultCache;
    final HttpCacheCounters counters;
    final SignalingExecutor executor;
    final LongObjectBiConsumer<Runnable> scheduler;

    public ProxyStreamFactory(
        RouteManager router,
        BudgetManager budgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        Long2ObjectHashMap<Request> requestCorrelations,
        Long2ObjectHashMap<ProxyConnectReplyStream> correlations,
        Long2ObjectHashMap<Future<?>> expiryRequestsCorrelations,
        Cache emulatedCache,
        DefaultCache defaultCache,
        HttpCacheCounters counters,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor,
        LongObjectBiConsumer<Runnable> scheduler)
    {
        this.router = requireNonNull(router);
        this.budgetManager = requireNonNull(budgetManager);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.requestBufferPool = new CountingBufferPool(
                requestBufferPool,
                counters.supplyCounter.apply("http-cache.request.acquires"),
                counters.supplyCounter.apply("http-cache.request.releases"));
        this.responseBufferPool = new CountingBufferPool(
                requestBufferPool,
                counters.supplyCounter.apply("http-cache.response.acquires"),
                counters.supplyCounter.apply("http-cache.response.releases"));
        this.requestCorrelations = requireNonNull(requestCorrelations);
        this.correlations = requireNonNull(correlations);
        this.expiryRequestsCorrelations = requireNonNull(expiryRequestsCorrelations);
        this.emulatedCache = emulatedCache;
        this.defaultCache = defaultCache;

        this.writer = new Writer(supplyTypeId, writeBuffer);
        this.counters = counters;
        this.executor = executor;
        this.scheduler = scheduler;
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer acceptReply)
    {
        final long acceptRouteId = begin.routeId();
        final long authorization = begin.authorization();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(acceptRouteId, authorization, filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptInitialId = begin.streamId();
            final long connectRouteId = route.correlationId();

            final OctetsFW extension = beginRO.extension();
            final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

            if(requestHeaders.anyMatch(HAS_EMULATED_PROTOCOL_STACK))
            {
                newStream = new EmulatedProxyAcceptStream(this, acceptReply, acceptRouteId, acceptInitialId,
                    connectRouteId)::handleStream;
            }
            else
            {
                long connectInitialId = supplyInitialId.applyAsLong(connectRouteId);
                MessageConsumer connectInitial = router.supplyReceiver(connectInitialId);
                long connectReplyId = supplyReplyId.applyAsLong(connectInitialId);
                long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
                MessageConsumer connectReply = router.supplyReceiver(connectReplyId);

                newStream = new ProxyAcceptStream(this,
                                                    acceptReply,
                                                    acceptRouteId,
                                                    acceptInitialId,
                                                    acceptReplyId,
                                                    connectInitial,
                                                    connectInitialId,
                                                    connectReplyId,
                                                    connectRouteId)::handleStream;

                ProxyConnectReplyStream replyStream = new ProxyConnectReplyStream(this,
                                                                                    connectReply,
                                                                                    connectRouteId,
                                                                                    connectReplyId,
                                                                                    acceptInitialId);
                correlations.put(connectReplyId, replyStream);
                router.setThrottle(acceptReplyId, replyStream::onThrottleBeforeBegin);
            }
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        final BeginFW begin,
        final MessageConsumer source)
    {
        final long sourceRouteId = begin.routeId();
        final long sourceId = begin.streamId();

        Request request = requestCorrelations.get(sourceId);

        if(request != null && request.isEmulated())
        {
            return new EmulatedProxyConnectReplyStream(this,
                                                        source,
                                                        sourceRouteId,
                                                        sourceId)::handleStream;
        }
        else
        {
            ProxyConnectReplyStream replyStream = correlations.remove(sourceId);
            MessageConsumer newStream = null;

            if (replyStream != null)
            {
                newStream = replyStream::handleStream;
            }

            return newStream;
        }
    }


    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    void initializeNewConnectReplyStream(long connectInitialId, long connectRouteId, long acceptInitialId)
    {
        long connectReplyId = supplyReplyId.applyAsLong(connectInitialId);
        MessageConsumer connectReply = router.supplyReceiver(connectReplyId);

        ProxyConnectReplyStream replyStream = new ProxyConnectReplyStream(this,
            connectReply,
            connectRouteId,
            connectReplyId,
            acceptInitialId);
        correlations.put(connectReplyId, replyStream);
    }

    boolean cleanupCorrelationIfNecessary(long  connectReplyId, long acceptInitialId)
    {
        final ProxyConnectReplyStream correlated = correlations.remove(connectReplyId);
        if (correlated != null)
        {
            router.clearThrottle(acceptInitialId);
        }

        return correlated != null;
    }
}
