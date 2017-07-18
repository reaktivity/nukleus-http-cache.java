/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.hasStoredResponseThatSatisfies;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getRequestURL;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
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
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> myRequestHeadersRO = new HttpBeginExFW().headers();
    private final ListFW<HttpHeaderFW> pendingRequestHeadersRO = new HttpBeginExFW().headers();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final AbortFW abortRO = new AbortFW();

    private final RouteHandler router;

    private final LongSupplier supplyStreamId;
    private final BufferPool bufferPool;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Writer writer;
    private final LongSupplier supplyCorrelationId;

    private final Long2ObjectHashMap<FanOut> junctions;

    public ProxyStreamFactory(
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations)
    {
        this.router = requireNonNull(router);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.bufferPool = requireNonNull(bufferPool);
        this.correlations = requireNonNull(correlations);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);

        this.writer = new Writer(writeBuffer);
        this.junctions = new Long2ObjectHashMap<>();
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
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer networkThrottle)
    {
        final long networkRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return networkRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ProxyAcceptStream(networkThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ProxyConnectReplyStream(throttle, throttleId)::handleStream;
    }

    public final class ProxyAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private String acceptName;
        private MessageConsumer acceptReply;
        private long acceptReplyStreamId;
        private final long acceptStreamId;
        private long acceptCorrelationId;

        private MessageConsumer connect;
        private String connectName;
        private long connectRef;
        private long connectCorrelationId;
        private long connectStreamId;

        private MessageConsumer streamState;

        private int myRequestSlot = NO_SLOT;
        private int requestSize;
        private Correlation streamCorrelation;
        private MessageConsumer connectReplyThrottle;
        private FanOut junction;
        private int requestURLHash;
        private long connectReplyStreamId;

        private ProxyAcceptStream(
                MessageConsumer acceptThrottle,
                long acceptStreamId)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptStreamId = acceptStreamId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                this.acceptName = begin.source().asString();
                handleBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                // just reset
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                this.connectName = connectRoute.target().asString();
                this.connect = router.supplyTarget(connectName);
                this.connectRef = connectRoute.targetRef();
                this.connectCorrelationId = supplyCorrelationId.getAsLong();

                this.acceptReply = router.supplyTarget(acceptName);
                this.acceptReplyStreamId = supplyStreamId.getAsLong();
                this.acceptCorrelationId = begin.correlationId();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

                // Should already be canonicalized in http / http2 nuklei
                final String requestURL = getRequestURL(requestHeaders);

                this.requestURLHash = requestURL.hashCode();

                if (!canBeServedByCache(requestHeaders))
                {
                    this.connectStreamId = supplyStreamId.getAsLong();
                    proxyRequest(requestHeaders);

                    router.setThrottle(connectName, connectStreamId, this::handleConnectThrottle);
                    this.streamState = this::afterProxyBegin;
                }
                else if (hasStoredResponseThatSatisfies(requestURL, requestURLHash, requestHeaders))
                {
                    // always false right now, so can't get here
                }
                else if (hasOutstandingRequestThatMaySatisfy(requestHeaders, requestURLHash))
                {
                    this.myRequestSlot = bufferPool.acquire(acceptStreamId);
                    storeRequest(requestHeaders, myRequestSlot);

                    this.junction = junctions.get(requestURLHash);
                    junction.addSubscriber(this::handleFanout);

                    // send 0 window back to complete handshake
                    writer.doWindow(acceptThrottle, acceptStreamId, 0, 0);

                    this.streamState = this::waitingForOutstanding;
                }
                else
                {
                    // create connection
                    this.connectStreamId = supplyStreamId.getAsLong();
                    int correlationRequestHeadersSlot = bufferPool.acquire(requestURLHash);
                    storeRequest(requestHeaders, correlationRequestHeadersSlot);

                    this.junction = new FanOut();

                    Correlation correlation = new Correlation(
                        requestURLHash,
                        junction,
                        bufferPool,
                        correlationRequestHeadersSlot,
                        requestSize,
                        true,
                        connectName,
                        connectRef);

                    correlations.put(connectCorrelationId, correlation);

                    junction.setStreamCorrelation(correlation);
                    junctions.put(requestURLHash, junction);
                    junction.addSubscriber(this::handleMyInitiatedFanout);

                    writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                    requestHeaders.forEach(
                            h -> e.item(h2 -> h2.representation((byte) 0).name(h.name()).value(h.value()))
                        )
                    );
                    writer.doHttpEnd(connect, connectStreamId);

                    // send 0 window back to complete handshake
                    writer.doWindow(acceptThrottle, acceptStreamId, 0, 0);

                    this.streamState = this::waitingForOutstanding;
                }
            }
        }

        private void proxyRequest(final ListFW<HttpHeaderFW> requestHeaders)
        {
            this.streamCorrelation = new Correlation(
                    requestURLHash,
                    this::proxyBack,
                    null,
                    NO_SLOT,
                    -1,
                    false,
                    connectName,
                    connectRef);

            correlations.put(connectCorrelationId, streamCorrelation);

            this.connectStreamId = supplyStreamId.getAsLong();

            writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                requestHeaders.forEach(h ->
                    e.item(h2 -> h2.representation((byte) 0).name(h.name())
                                .value(h.value()))
                )
            );
        }

        private boolean handleMyInitiatedFanout(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch(msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
                    writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                        responseHeaders.forEach(h ->
                            e.item(h2 -> h2.representation((byte) 0)
                                           .name(h.name())
                                           .value(h.value().asString()))
                        )
                    );
                    router.setThrottle(acceptName, acceptReplyStreamId, junction.getHandleAcceptReplyThrottle());
                    break;
                default:
                    this.proxyBackAfterBegin(msgTypeId, buffer, index, length);
            }
            return true;
        }

        private boolean handleFanout(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();

                    this.streamCorrelation = this.junction.getStreamCorrelation();

                    // both user buffer pool and buffer pool counters are singletons...
                    Supplier<ListFW<HttpHeaderFW>> pendingRequestHeaders = () ->
                    {
                        return streamCorrelation.headers(pendingRequestHeadersRO);
                    };

                    Supplier<ListFW<HttpHeaderFW>> myRequestHeaders = () ->
                    {
                        return getRequestHeaders(myRequestHeadersRO);
                    };

                    if (responseCanSatisfyRequest(pendingRequestHeaders, myRequestHeaders, responseHeaders))
                    {
                        writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                            responseHeaders.forEach(h ->
                                e.item(h2 -> h2.representation((byte) 0).name(h.name()).value(h.value()))
                            )
                        );
                        router.setThrottle(acceptName, acceptReplyStreamId, junction.getHandleAcceptReplyThrottle());
                        return true;
                    }
                    else
                    {
                        this.junction = null;
                        this.connectCorrelationId = supplyCorrelationId.getAsLong();
                        proxyRequest(myRequestHeaders.get());
                        writer.doHttpEnd(connect, connectStreamId);
                        return false;
                    }
                default:
                    proxyBackAfterBegin(msgTypeId, buffer, index, length);
                    return true;
            }
        }

        private void proxyBack(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
                    // DPW should inject Push Promise?
                    writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                        responseHeaders.forEach(h -> e.item(
                                h2 -> h2.representation((byte) 0)
                                        .name(h.name())
                                        .value(h.value())))
                    );
                    this.connectReplyStreamId = streamCorrelation.getConnectReplyStreamId();
                    this.connectReplyThrottle = streamCorrelation.getConnectReplyThrottle();
                    router.setThrottle(acceptName, acceptReplyStreamId, this::handleAcceptReplyThrottle);
                    break;
                default:
                    proxyBackAfterBegin(msgTypeId, buffer, index, length);
                    break;
            }
        }

        private void proxyBackAfterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    dataRO.wrap(buffer, index, index + length);
                    OctetsFW payload = dataRO.payload();
                    writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
                    break;
                case EndFW.TYPE_ID:
                    writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                    clean();
                    break;
                case AbortFW.TYPE_ID:
                    writer.doAbort(acceptReply, acceptReplyStreamId);
                    clean();
                    break;
                default:
                    break;
            }
        }

        private boolean responseCanSatisfyRequest(
                final Supplier<ListFW<HttpHeaderFW>> pendingRequestHeaders,
                final Supplier<ListFW<HttpHeaderFW>> myRequestHeaders,
                final ListFW<HttpHeaderFW> responseHeaders)
        {

            final String vary = getHeader(responseHeaders, "vary");
            final String cacheControl = getHeader(responseHeaders, "cache-control");

            final String pendingRequestAuthorizationHeader = getHeader(pendingRequestHeaders.get(), "authorization");

            final String myAuthorizationHeader = getHeader(myRequestHeaders.get(), "authorization");

            boolean useSharedResponse = true;

            if (cacheControl != null && cacheControl.contains("public"))
            {
                useSharedResponse = true;
            }
            else if (cacheControl != null && cacheControl.contains("private"))
            {
                useSharedResponse = false;
            }
            else if (myAuthorizationHeader != null || pendingRequestAuthorizationHeader != null)
            {
                useSharedResponse = false;
            }
            else if (vary != null)
            {
                useSharedResponse = stream(vary.split("\\s*,\\s*")).anyMatch(v ->
                {
                    String pendingHeaderValue = getHeader(pendingRequestHeaders.get(), v);
                    String myHeaderValue = getHeader(myRequestHeaders.get(), v);
                    return Objects.equals(pendingHeaderValue, myHeaderValue);
                });
            }

            return useSharedResponse;
        }

        private void clean()
        {
            if (this.myRequestSlot != NO_SLOT)
            {
                bufferPool.release(this.myRequestSlot);
            }
        }

        private ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> headersRO)
        {
            return headersRO.wrap(bufferPool.buffer(this.myRequestSlot), 0, requestSize);
        }

        private int storeRequest(final ListFW<HttpHeaderFW> headers, int requestCacheSlot)
        {
            this.requestSize = 0;
            MutableDirectBuffer requestCacheBuffer = bufferPool.buffer(requestCacheSlot);
            headers.forEach(h ->
            {
                requestCacheBuffer.putBytes(this.requestSize, h.buffer(), h.offset(), h.sizeof());
                this.requestSize += h.sizeof();
            });
            return this.requestSize;
        }

        private void afterProxyBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                writer.doReset(acceptThrottle, acceptStreamId);
                break;
            }
        }

        private void handleData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void handleEnd(
                EndFW end)
        {
            writer.doHttpEnd(connect, connectStreamId);
        }

        private void handleAbort(
                AbortFW abort)
        {
            writer.doAbort(connect, connectStreamId);
        }

        private void waitingForOutstanding(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                // TODO H2 late headers?? RFC might say can't affect caching, but
                // probably should still forward should expected request not match...
                break;
            case DataFW.TYPE_ID:
            default:
                writer.doReset(acceptThrottle, acceptStreamId);
                break;
            }
        }

        private boolean hasOutstandingRequestThatMaySatisfy(
            ListFW<HttpHeaderFW> requestHeaders,
            int requestURLHash)
        {
            if (junctions.containsKey(requestURLHash))
            {
                return requestHeaders.anyMatch(h ->
                {
                    final String name = h.name().asString();
                    final String value = h.value().asString();
                    return "x-http-cache-sync".equals(name) && "always".equals(value);
                });
            }
            return false;
        }

        private void handleAcceptReplyThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleAcceptReplyWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleAcceptReplyReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleAcceptReplyWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(connectReplyThrottle, this.connectReplyStreamId, bytes, frames);
        }

        private void handleAcceptReplyReset(
            ResetFW reset)
        {
            writer.doReset(connectReplyThrottle, this.connectReplyStreamId);
        }

        private void handleConnectThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(acceptThrottle, acceptStreamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, acceptStreamId);
        }

    }

    private final class ProxyConnectReplyStream
    {
        private MessageConsumer streamState;

        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyStreamId;

        private ProxyConnectReplyStream(
                MessageConsumer connectReplyThrottle,
                long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyStreamId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long connectReplyRef = begin.sourceRef();
            final long connectCorrelationId = begin.correlationId();

            final Correlation streamCorrelation = connectReplyRef == 0L ?
                correlations.remove(connectCorrelationId) : null;

            if (streamCorrelation != null)
            {
                if (streamCorrelation.follow304())
                {
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
                    final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.headers(pendingRequestHeadersRO);
                    if (responseHeaders.anyMatch(h -> ":status".equals(h.name().asString())
                                                && "304".equals(h.value().asString())) &&
                        requestHeaders.anyMatch(h -> "x-poll-injected".equals(h.name().asString()))
                        )
                    {
                        writer.doWindow(connectReplyThrottle, connectReplyStreamId, 0, 0);
                        final String connectName = streamCorrelation.connectName();
                        final long connectRef = streamCorrelation.connectRef();
                        final MessageConsumer newConnect = router.supplyTarget(connectName);
                        final long newConnectStreamId = supplyStreamId.getAsLong();
                        final long newConnectCorrelationId = supplyCorrelationId.getAsLong();
                        writer.doHttpBegin(newConnect, newConnectStreamId, connectRef, newConnectCorrelationId, e ->
                            requestHeaders.forEach(
                                    h -> e.item(h2 -> h2.representation((byte) 0).name(h.name()).value(h.value()))
                                )
                            );
                        correlations.put(newConnectCorrelationId, streamCorrelation);
                        writer.doHttpEnd(newConnect, newConnectStreamId);
                        streamState = this::ignoreRest;
                    }
                    else
                    {
                        forwardConnectReply(begin, streamCorrelation);
                    }
                }
                else
                {
                    forwardConnectReply(begin, streamCorrelation);
                }
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void forwardConnectReply(BeginFW begin, final Correlation streamCorrelation)
        {
            streamCorrelation.setConnectReplyThrottle(connectReplyThrottle);
            streamCorrelation.setConnectReplyStreamId(connectReplyStreamId);
            final MessageConsumer consumer = streamCorrelation.consumer();
            consumer.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
            streamState = consumer;
        }

        private void ignoreRest(int msgTypeId, DirectBuffer buffer, int index, int length)
        {
            // NOOP
        }
    }

    private final class FanOut implements MessageConsumer
    {

        private final Set<BooleanMessageConsumer> outs;
        private Correlation streamCorrelation;
        private GroupThrottle connectReplyThrottle;
        private long connectReplyStreamId;

        FanOut()
        {
            this.outs = new HashSet<>();
        }

        public MessageConsumer getHandleAcceptReplyThrottle()
        {
            return this::handleAcceptThrottle;
        }

        public void setStreamCorrelation(Correlation correlation)
        {
            this.streamCorrelation = correlation;
        }

        public Correlation getStreamCorrelation()
        {
            return this.streamCorrelation;
        }

        private void addSubscriber(BooleanMessageConsumer out)
        {
            outs.add(out);
        }

        @Override
        public void accept(int msgTypeId, DirectBuffer buffer, int index, int length)
        {

            for (Iterator<BooleanMessageConsumer> i = outs.iterator(); i.hasNext();)
            {
                BooleanMessageConsumer messageConsumer = i.next();
                if (!messageConsumer.accept(msgTypeId, buffer, index, length))
                {
                    i.remove();
                }

                final MessageConsumer connectReply = streamCorrelation.getConnectReplyThrottle();
                this.connectReplyStreamId = streamCorrelation.getConnectReplyStreamId();
                this.connectReplyThrottle = new GroupThrottle(outs.size(), writer, connectReply, connectReplyStreamId);
            }

            switch(msgTypeId)
            {
                case EndFW.TYPE_ID:
                case AbortFW.TYPE_ID:
                    this.streamCorrelation.cleanUp();
                    break;
                default:
            }
        }

        public void handleAcceptThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();
            final long streamId = window.streamId();
            connectReplyThrottle.processWindow(streamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            final long streamId = reset.streamId();
            connectReplyThrottle.processReset(streamId);
        }
    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() && sourceName.equals(route.source().asString());
        };

        return router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }
}