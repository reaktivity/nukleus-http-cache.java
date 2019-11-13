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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.emulated.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
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

public class HttpCacheProxyFactory implements StreamFactory
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
    final ArrayFW<HttpHeaderFW> requestHeadersRO = new ArrayFW<>(new HttpHeaderFW());

    final RouteManager router;
    final Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations;
    final EmulatedBudgetManager emulatedBudgetManager;

    final LongUnaryOperator supplyInitialId;
    final LongUnaryOperator supplyReplyId;
    final LongSupplier supplyTraceId;
    final ToIntFunction<String> supplyTypeId;
    final LongFunction<BudgetDebitor> supplyDebitor;
    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;
    final MutableDirectBuffer writeBuffer;
    final Long2ObjectHashMap<Request> requestCorrelations;

    final int preferWaitMaximum;
    final Writer writer;
    final CacheControl cacheControlParser = new CacheControl();
    final Cache emulatedCache;
    final DefaultCache defaultCache;
    final HttpCacheCounters counters;
    final SignalingExecutor executor;
    final LongObjectBiConsumer<Runnable> scheduler;

    public final Int2ObjectHashMap<HttpProxyCacheableRequestGroup> requestGroups;

    public HttpCacheProxyFactory(
        HttpCacheConfiguration config,
        RouteManager router,
        EmulatedBudgetManager emulatedBudgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongFunction<BudgetDebitor> supplyDebitor,
        Long2ObjectHashMap<Request> requestCorrelations,
        Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations,
        Cache emulatedCache,
        DefaultCache defaultCache,
        HttpCacheCounters counters,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor,
        LongObjectBiConsumer<Runnable> scheduler)
    {
        this.router = requireNonNull(router);
        this.emulatedBudgetManager = requireNonNull(emulatedBudgetManager);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.preferWaitMaximum = config.preferWaitMaximum();
        this.supplyTypeId = supplyTypeId;
        this.supplyDebitor = supplyDebitor;
        this.requestBufferPool = new CountingBufferPool(
            requestBufferPool,
            counters.supplyCounter.apply("http-cache.request.acquires"),
            counters.supplyCounter.apply("http-cache.request.releases"));
        this.responseBufferPool = new CountingBufferPool(
                requestBufferPool,
                counters.supplyCounter.apply("http-cache.response.acquires"),
                counters.supplyCounter.apply("http-cache.response.releases"));
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);

        this.requestCorrelations = requireNonNull(requestCorrelations);
        this.correlations = requireNonNull(correlations);
        this.emulatedCache = emulatedCache;
        this.defaultCache = defaultCache;

        this.writer = new Writer(router, supplyTypeId, writeBuffer);
        this.requestGroups = new Int2ObjectHashMap<>();
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
        MessageConsumer source)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, source);
        }
        else
        {
            newStream = newReplyStream(begin, source);
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
            final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

            if (requestHeaders.anyMatch(HAS_EMULATED_PROTOCOL_STACK))
            {
                newStream = new EmulatedProxyAcceptStream(this,
                                                          acceptReply,
                                                          acceptRouteId,
                                                          acceptInitialId,
                                                          connectRouteId)::handleStream;
            }
            else
            {
                newStream = newNativeInitialStream(requestHeaders,
                                                   acceptReply,
                                                   authorization,
                                                   connectRouteId,
                                                   acceptInitialId,
                                                   acceptRouteId,
                                                   begin.traceId());
            }
        }

        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer connectInitial)
    {
        final long sourceRouteId = begin.routeId();
        final long sourceId = begin.streamId();

        Request request = requestCorrelations.get(sourceId);
        MessageConsumer newStream = null;

        if (request != null && request.isEmulated())
        {
            return new EmulatedProxyConnectReplyStream(this,
                                                        connectInitial,
                                                        sourceRouteId,
                                                        sourceId)::handleStream;
        }
        else
        {
            Function<HttpBeginExFW, MessageConsumer> newResponse = correlations.remove(sourceId);
            if (newResponse != null)
            {
                final OctetsFW extension = begin.extension();
                final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::tryWrap);
                if (httpBeginFW != null)
                {
                    newStream = newResponse.apply(httpBeginFW);
                }
            }
        }

        return newStream;
    }

    private MessageConsumer newNativeInitialStream(
        ArrayFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        long authorization,
        long connectRouteId,
        long acceptInitialId,
        long acceptRouteId,
        long traceId)
    {
        final String requestURL = getRequestURL(requestHeaders);
        final short authorizationScope = authorizationScope(authorization);
        final int requestHash = RequestUtil.requestHash(authorizationScope, requestURL.hashCode());
        final long connectInitialId = supplyInitialId.applyAsLong(connectRouteId);
        final MessageConsumer connectInitial = router.supplyReceiver(connectInitialId);
        final long connectReplyId = supplyReplyId.applyAsLong(connectInitialId);
        final long acceptReplyId = supplyReplyId.applyAsLong(acceptInitialId);
        final MessageConsumer connectReply = router.supplyReceiver(connectReplyId);
        MessageConsumer newStream = null;
        counters.requests.getAsLong();

        if (defaultCache.matchCacheableRequest(requestHeaders, authorizationScope, requestHash))
        {
            newStream = createCachedStream(requestHeaders,
                                           acceptReply,
                                           acceptInitialId,
                                           acceptRouteId,
                                           requestHash,
                                           acceptReplyId);
        }
        else if (requestHeaders.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED))
        {
            handIsOnlyCached(acceptReply,
                             acceptInitialId,
                             acceptRouteId,
                             traceId,
                             acceptReplyId);
        }
        else if (defaultCache.isRequestCacheable(requestHeaders))
        {
            newStream = createCacheableRequestStream(requestHeaders,
                                                     acceptReply,
                                                     connectRouteId,
                                                     acceptInitialId,
                                                     acceptRouteId,
                                                     requestURL,
                                                     requestHash,
                                                     connectInitialId,
                                                     connectInitial,
                                                     connectReplyId,
                                                     acceptReplyId);
        }
        else
        {
            newStream = createNonCacheableRequestStream(acceptReply,
                                                        connectRouteId,
                                                        acceptInitialId,
                                                        acceptRouteId,
                                                        requestURL,
                                                        requestHash,
                                                        connectInitialId,
                                                        connectInitial,
                                                        connectReplyId,
                                                        acceptReplyId,
                                                        connectReply);
        }

        return newStream;
    }

    private MessageConsumer createNonCacheableRequestStream(
        MessageConsumer acceptReply,
        long connectRouteId,
        long acceptInitialId,
        long acceptRouteId,
        String requestURL,
        int requestHash,
        long connectInitialId,
        MessageConsumer connectInitial,
        long connectReplyId,
        long acceptReplyId,
        MessageConsumer connectReply)
    {
        MessageConsumer newStream;
        final HttpCacheProxyNonCacheableRequest nonCacheableRequest =
            new HttpCacheProxyNonCacheableRequest(this,
                                                  requestHash,
                                                  requestURL,
                                                  acceptReply,
                                                  acceptRouteId,
                                                  acceptReplyId,
                                                  acceptInitialId,
                                                  connectInitial,
                                                  connectReply,
                                                  connectInitialId,
                                                  connectReplyId,
                                                  connectRouteId);
        newStream = nonCacheableRequest::onRequestMessage;
        correlations.put(connectReplyId, nonCacheableRequest::newResponse);
        router.setThrottle(acceptReplyId, nonCacheableRequest::onResponseMessage);
        return newStream;
    }

    private MessageConsumer createCacheableRequestStream(
        ArrayFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        long connectRouteId,
        long acceptInitialId,
        long acceptRouteId,
        String requestURL,
        int requestHash,
        long connectInitialId,
        MessageConsumer connectInitial,
        long connectReplyId,
        long acceptReplyId)
    {
        counters.requestsCacheable.getAsLong();
        MessageConsumer newStream;
        HttpProxyCacheableRequestGroup group = requestGroups.computeIfAbsent(requestHash, this::newCacheableRequestGroup);

        HttpHeaderFW authorizationHeader = requestHeaders.matchFirst(h -> AUTHORIZATION.equals(h.name().asString()));
        if (authorizationHeader != null)
        {
            group.setRecentAuthorizationToken(authorizationHeader.value().asString());
        }
        final HttpCacheProxyCacheableRequest cacheableRequest =
            new HttpCacheProxyCacheableRequest(this,
                                                group,
                                                requestHash,
                                                requestURL,
                                                acceptReply,
                                                acceptRouteId,
                                                acceptInitialId,
                                                acceptReplyId,
                                                connectInitial,
                                                connectInitialId,
                                                connectReplyId,
                                                connectRouteId);
        correlations.put(connectReplyId, cacheableRequest::newResponse);
        newStream = cacheableRequest::onAcceptMessage;
        router.setThrottle(acceptReplyId, newStream);
        return newStream;
    }

    private void handIsOnlyCached(
        MessageConsumer acceptReply,
        long acceptInitialId,
        long acceptRouteId,
        long traceId,
        long acceptReplyId)
    {
        counters.requestsCacheable.getAsLong();
        writer.doWindow(acceptReply,
                        acceptRouteId,
                        acceptInitialId,
                        traceId,
                        0L,
                        0,
                        0);
        send504(acceptReply, acceptRouteId, acceptReplyId, supplyTraceId.getAsLong());
    }

    private MessageConsumer createCachedStream(
        ArrayFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        long acceptInitialId,
        long acceptRouteId,
        int requestHash,
        long acceptReplyId)
    {
        counters.requestsCacheable.getAsLong();
        counters.responsesCached.getAsLong();
        MessageConsumer newStream;
        DefaultCacheEntry cacheEntry = defaultCache.get(requestHash);
        boolean etagMatched = CacheUtils.isMatchByEtag(requestHeaders, cacheEntry.etag());
        if (etagMatched)
        {
            final HttpCacheProxyCachedNotModifiedRequest cachedNotModifiedRequest =
                new HttpCacheProxyCachedNotModifiedRequest(this,
                                                          acceptReply,
                                                          acceptRouteId,
                                                          acceptReplyId,
                                                          acceptInitialId);
            newStream = cachedNotModifiedRequest::onAccept;
            router.setThrottle(acceptReplyId, newStream);
        }
        else
        {
            final HttpCacheProxyCachedRequest cachedRequest =
                new HttpCacheProxyCachedRequest(this,
                                                requestHash,
                                                acceptReply,
                                                acceptRouteId,
                                                acceptReplyId,
                                                acceptInitialId);
            newStream = cachedRequest::onAccept;
            router.setThrottle(acceptReplyId, newStream);
        }
        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private void send504(
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        long trace)
    {

        writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, trace, e ->
            e.item(h -> h.name(STATUS).value("504")));
        writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, trace);

        // count all responses
        counters.responses.getAsLong();
    }

    private HttpProxyCacheableRequestGroup newCacheableRequestGroup(int requestHash)
    {
        return new HttpProxyCacheableRequestGroup(requestHash, writer, this, requestGroups::remove);
    }
}
