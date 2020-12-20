/**
 * Copyright 2016-2020 The Reaktivity Project
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
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.requestHash;

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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
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

    final HttpBeginExFW defaultHttpBeginExRO;
    final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    final HttpEndExFW httpEndExRO = new HttpEndExFW();
    final Array32FW<HttpHeaderFW> httpHeadersRO = new Array32FW<>(new HttpHeaderFW());

    final BeginFW.Builder beginRW = new BeginFW.Builder();

    final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    final Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> httpHeadersRW =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());

    final RouteManager router;
    final Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations;
    final Int2ObjectHashMap<HttpProxyCacheableRequestGroup> requestGroups;

    final LongUnaryOperator supplyInitialId;
    final LongUnaryOperator supplyReplyId;
    final LongSupplier supplyTraceId;
    final ToIntFunction<String> supplyTypeId;
    final LongFunction<BudgetDebitor> supplyDebitor;
    final BufferPool headersPool;
    final MutableDirectBuffer writeBuffer;

    final Writer writer;
    final DefaultCache defaultCache;
    final HttpCacheCounters counters;
    final SignalingExecutor executor;
    final int preferWaitMaximum;
    final int initialWindowSize;

    public HttpCacheProxyFactory(
        HttpCacheConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongFunction<BudgetDebitor> supplyDebitor,
        Long2ObjectHashMap<Function<HttpBeginExFW, MessageConsumer>> correlations,
        DefaultCache defaultCache,
        HttpCacheCounters counters,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor)
    {
        this.router = requireNonNull(router);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.preferWaitMaximum = config.preferWaitMaximum();
        this.initialWindowSize = config.initialWindowSize();
        this.supplyTypeId = supplyTypeId;
        this.supplyDebitor = supplyDebitor;
        this.headersPool = new CountingBufferPool(
            requestBufferPool,
            counters.supplyCounter.apply("http-cache.request.acquires"),
            counters.supplyCounter.apply("http-cache.request.releases"));
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);

        this.correlations = requireNonNull(correlations);
        this.defaultCache = defaultCache;

        this.writer = new Writer(supplyTypeId, writeBuffer);
        this.requestGroups = new Int2ObjectHashMap<>();
        this.counters = counters;
        this.executor = executor;

        this.defaultHttpBeginExRO = new HttpBeginExFW.Builder()
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
            .typeId(supplyTypeId.applyAsInt("http"))
            .headersItem(h -> h.name(STATUS).value("500"))
            .build();

    }

    public HttpProxyCacheableRequestGroup getRequestGroup(
        int requestHash)
    {
        return requestGroups.get(requestHash);
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
            newStream = newRequestStream(begin, source);
        }
        else
        {
            newStream = newResponseStream(begin);
        }

        return newStream;
    }

    private MessageConsumer newRequestStream(
        final BeginFW begin,
        final MessageConsumer initial)
    {
        final long routeId = begin.routeId();
        final long authorization = begin.authorization();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, authorization, filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long resolveId = route.correlationId();
            final long initialId = begin.streamId();
            final long traceId = begin.traceId();
            final OctetsFW extension = begin.extension();
            final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
            final Array32FW<HttpHeaderFW> headers = httpBeginFW.headers();

            newStream = newNativeRequestStream(initial,
                                               routeId,
                                               initialId,
                                               traceId,
                                               authorization,
                                               resolveId,
                                               headers);

        }

        return newStream;
    }

    private MessageConsumer newResponseStream(
        final BeginFW begin)
    {
        final long sourceId = begin.streamId();

        MessageConsumer newStream = null;

        Function<HttpBeginExFW, MessageConsumer> newResponse = correlations.remove(sourceId);
        if (newResponse != null)
        {
            final OctetsFW extension = begin.extension();
            HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::tryWrap);
            if (httpBeginEx == null)
            {
                httpBeginEx = defaultHttpBeginExRO;
            }
            newStream = newResponse.apply(httpBeginEx);
        }

        return newStream;
    }

    private MessageConsumer newNativeRequestStream(
        MessageConsumer initial,
        long routeId,
        long initialId,
        long traceId,
        long authorization,
        long resolveId,
        Array32FW<HttpHeaderFW> headers)
    {
        final String requestURL = getRequestURL(headers);
        final boolean isMethodUnsafe = CacheUtils.isMethodUnsafe(headers);
        final short authorizationScope = authorizationScope(authorization);
        final int requestHash = requestHash(authorizationScope, requestURL.hashCode());

        MessageConsumer newStream = null;

        final boolean isRequestCacheable = defaultCache.isRequestCacheable(headers);
        final boolean matchCacheableRequest = defaultCache.matchCacheableRequest(headers, authorizationScope, requestHash);
        DefaultCacheEntry cacheEntry = defaultCache.get(requestHash);

        if (isRequestCacheable &&
            matchCacheableRequest &&
            CacheUtils.isMatchByEtag(headers, cacheEntry.etag()))
        {
            final HttpCacheProxyCachedNotModifiedRequest cachedNotModifiedRequest =
                new HttpCacheProxyCachedNotModifiedRequest(
                    this,
                    initial,
                    routeId,
                    initialId,
                    cacheEntry);
            newStream = cachedNotModifiedRequest::onRequestMessage;
        }
        else if (headers.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED) && !matchCacheableRequest)
        {
            handleOnlyIfCachedRequest(initial,
                routeId,
                initialId,
                traceId);
        }
        else if (isRequestCacheable)
        {
            if (defaultCache.isCacheFull())
            {
                defaultCache.purgeEntriesForNonPendingRequests(requestGroups.keySet());
            }

            if (!defaultCache.isCacheFull())
            {
                HttpProxyCacheableRequestGroup group = supplyCacheableRequestGroup(requestHash);

                HttpHeaderFW authorizationHeader = headers.matchFirst(h -> AUTHORIZATION.equals(h.name().asString()));
                if (authorizationHeader != null)
                {
                    group.authorizationHeader(authorizationHeader.value().asString());
                }
                newStream = newCacheableRequestStream(
                    initial,
                    routeId,
                    initialId,
                    resolveId,
                    group);
            }
            else
            {
                newStream = newNonCacheableRequestStream(
                    initial,
                    routeId,
                    initialId,
                    resolveId,
                    requestURL,
                    requestHash,
                    isMethodUnsafe);
            }
        }
        else
        {
            newStream = newNonCacheableRequestStream(
                initial,
                routeId,
                initialId,
                resolveId,
                requestURL,
                requestHash,
                isMethodUnsafe);
        }
        counters.requests.getAsLong();

        return newStream;
    }

    private MessageConsumer newNonCacheableRequestStream(
        MessageConsumer initial,
        long routeId,
        long initialId,
        long resolveId,
        String requestURL,
        int requestHash,
        boolean isMethodUnsafe)
    {
        final HttpCacheProxyNonCacheableRequest nonCacheableRequest =
            new HttpCacheProxyNonCacheableRequest(this,
                                                  initial,
                                                  routeId,
                                                  initialId,
                                                  resolveId,
                                                  requestHash,
                                                  requestURL,
                                                  isMethodUnsafe);
        final MessageConsumer newStream = nonCacheableRequest::onRequestMessage;
        router.setThrottle(nonCacheableRequest.replyId, nonCacheableRequest::onResponseMessage);
        return newStream;
    }

    private MessageConsumer newCacheableRequestStream(
        MessageConsumer initial,
        long routeId,
        long initialId,
        long resolveId,
        HttpProxyCacheableRequestGroup group)
    {
        final HttpCacheProxyCacheableRequest cacheableRequest =
            new HttpCacheProxyCacheableRequest(this,
                                               group,
                                               initial,
                                               routeId,
                                               initialId,
                                               resolveId);
        final MessageConsumer newStream = cacheableRequest::onRequestMessage;
        router.setThrottle(cacheableRequest.replyId, cacheableRequest::onResponseMessage);
        return newStream;
    }

    private void handleOnlyIfCachedRequest(
        MessageConsumer initial,
        long routeId,
        long initialId,
        long traceId)
    {
        counters.requestsCacheable.getAsLong();
        writer.doWindow(initial,
                        routeId,
                        initialId,
                        0L,
                        0L,
                        0,
                        traceId,
                        0L,
                        0);

        final long replyId = supplyReplyId.applyAsLong(initialId);
        send504(initial, routeId, replyId, traceId);
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
        long traceId)
    {
        writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, traceId,
            e -> e.item(h -> h.name(STATUS).value("504")));
        writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, traceId);

        // count all responses
        counters.responses.getAsLong();
    }

    private HttpProxyCacheableRequestGroup supplyCacheableRequestGroup(
        int requestHash)
    {
        return requestGroups.computeIfAbsent(requestHash, this::newCacheableRequestGroup);
    }

    private HttpProxyCacheableRequestGroup newCacheableRequestGroup(
        int requestHash)
    {
        counters.requestGroups.accept(1);
        return new HttpProxyCacheableRequestGroup(this, requestGroups::remove, requestHash);
    }
}
