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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.hasStoredResponseThatSatisfies;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getRequestURL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
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
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> headersRO = new HttpBeginExFW().headers();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final RouteHandler router;

    private final LongSupplier supplyStreamId;
    private final BufferPool bufferPool;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Writer writer;
    private final LongSupplier supplyCorrelationId;

    public final Long2ObjectHashMap<ProxyAcceptStream> urlToPendingStream;
    private final Int2ObjectHashMap<List<ProxyAcceptStream>> awaitingRequestMatches;

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
        this.urlToPendingStream = new Long2ObjectHashMap<>();
        this.awaitingRequestMatches = new Int2ObjectHashMap<>();
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
        private long acceptCorrelationId;

        private MessageConsumer connect;
        private long connectRef;
        private long connectCorrelationId;
        private long connectStreamId;

        private MessageConsumer streamState;

        private int requestCacheSlot = NO_SLOT;
        private int requestSize;
        private boolean usePendingRequest;
        private MessageConsumer responseMessageHandler;
        private int requestURLHash;
        private GroupThrottle groupThrottle;

        private ProxyAcceptStream(
                MessageConsumer connectThrottle,
                long connectStreamId)
        {
            this.acceptThrottle = connectThrottle;
            this.connectStreamId = connectStreamId;
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
                writer.doReset(acceptThrottle, connectStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final long acceptStreamId = beginRO.streamId();
            this.acceptName = begin.source().asString();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                // just reset
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                final String connectName = connectRoute.target().asString();
                this.connect = router.supplyTarget(connectName);
                this.connectRef = connectRoute.targetRef();
                this.connectCorrelationId = supplyCorrelationId.getAsLong();

                this.acceptReply = router.supplyTarget(acceptName);
                this.acceptCorrelationId = begin.correlationId();
                this.acceptReplyStreamId = supplyStreamId.getAsLong();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> headers = httpBeginFW.headers();

                final String requestURL = getRequestURL(headers); // TODO canonicalize or will http / http2 nuklei do that for us
                this.requestURLHash = requestURL.hashCode();

                if (!canBeServedByCache(headers))
                {
                    final Correlation correlation =
                            new Correlation(acceptName, acceptCorrelationId, requestURLHash, awaitingRequestMatches);
                    correlations.put(connectCorrelationId, correlation);
                    writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                        headers.forEach(h ->
                            e.item(h2 -> h2.name(h.name().asString()).value(h.value().asString()))
                        )
                    );
                    router.setThrottle(connectName, connectStreamId, this::handleThrottle);
                    this.streamState = this::afterBegin;
                }


                else if (hasStoredResponseThatSatisfies(requestURL, requestURLHash, headers))
                {
                    // always false right now, so can't get here
                }


                else if (hasOutstandingRequestThatMaySatisfy(headers, requestURLHash))
                {
                    List<ProxyAcceptStream> awaitingRequests = awaitingRequestMatches
                                                              .getOrDefault(requestURLHash, new ArrayList<ProxyAcceptStream>());
                    awaitingRequestMatches.put(requestURLHash, awaitingRequests);
                    this.requestCacheSlot = bufferPool.acquire(acceptStreamId);
                    storeRequest(headers, requestCacheSlot);
                    this.responseMessageHandler = this::handleReply;
                    this.usePendingRequest = true;
                    awaitingRequests.add(this);
                    this.streamState = this::waitingForOutstanding;
                }


                else
                {
                    boolean canBeUsedForPendingStream = !urlToPendingStream.containsKey(requestURLHash);
                    if (canBeUsedForPendingStream)
                    {
                        this.requestCacheSlot = bufferPool.acquire(requestURLHash);
                        storeRequest(headers, requestCacheSlot);
                        urlToPendingStream.put(requestURLHash, this);
                    }
                    final Correlation correlation =
                            new Correlation(acceptName, acceptCorrelationId, requestURLHash, awaitingRequestMatches);
                    correlations.put(connectCorrelationId, correlation);
                    writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                        headers.forEach(
                            h -> e.item(h2 -> h2.name(h.name().asString()).value(h.value().asString()))
                        )
                    );
                    router.setThrottle(connectName, connectStreamId, this::handleThrottle);
                    this.streamState = this::afterBegin;
                }
            }
        }

        private void handleReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (usePendingRequest)
            {
                switch (msgTypeId)
                {
                    case BeginFW.TYPE_ID:
                        processBeginReply(buffer, index, length);
                        break;
                    case DataFW.TYPE_ID:
                        dataRO.wrap(buffer, index, index + length);
                        OctetsFW payload = dataRO.payload();
                        writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
                        break;
                    case EndFW.TYPE_ID:
                        writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                        break;
                    default:
                        // TODO ABORT??
                        break;
                }
            }
        }

        private void processBeginReply(DirectBuffer buffer, int index, int length)
        {

            beginRO.wrap(buffer, index, index + length);
            final OctetsFW extension = beginRO.extension();
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();

            final String vary = getHeader(responseHeaders, "vary");
            final String cacheControl = getHeader(responseHeaders, "cache-control");

            ProxyAcceptStream pendingRequest = urlToPendingStream.get(requestURLHash);

            pendingRequest.getRequestHeaders(headersRO);
            final String pendingRequestAuthorizationHeader = getHeader(headersRO, "authorization");

            getRequestHeaders(headersRO);
            final String myAuthorizationHeader = getHeader(headersRO, "authorization");

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
                useSharedResponse = Arrays.stream(vary.split("\\s*,\\s*")).anyMatch(v ->
                {
                    pendingRequest.getRequestHeaders(headersRO);
                    String pendingHeaderValue = HttpHeadersUtil.getHeader(headersRO, v);
                    getRequestHeaders(headersRO);
                    String myHeaderValue = HttpHeadersUtil.getHeader(headersRO, v);
                    return Objects.equals(pendingHeaderValue, myHeaderValue);
                });
            }

            if (useSharedResponse)
            {
                router.setThrottle(acceptName, acceptReplyStreamId, this::handleGroupThrottle);
                writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                    responseHeaders.forEach(
                        h -> e.item(h2 -> h2.representation((byte)0).name(h.name().asString()).value(h.value().asString()))
                    )
                );
                clean();
            }
            else
            {
                this.groupThrottle.optOut();
                this.usePendingRequest = false;
                // TODO, consider allowing use in next cache, timing on clearing out stored
                // request is needed for this to work though
                final ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders(headersRO);
                writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                    requestHeaders.forEach(h ->
                        e.item(h2 -> h2.name(h.name().asString()).value(h.value().asString()))
                    )
                );

                Correlation correlation =
                        new Correlation(acceptName, acceptCorrelationId, requestURLHash, awaitingRequestMatches);
                correlations.put(connectCorrelationId, correlation);

                writer.doHttpEnd(connect, connectStreamId);
                clean();
            }
        }

        private void handleGroupThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    this.groupThrottle.processWindow(window.streamId(), window.update());
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    this.groupThrottle.processReset(reset.streamId());
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void clean()
        {
            if (this.requestCacheSlot != NO_SLOT)
            {
                bufferPool.release(this.requestCacheSlot);
            }
        }

        private ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> headersRO)
        {
            return headersRO.wrap(bufferPool.buffer(this.requestCacheSlot), 0, requestSize);
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
                writer.doReset(acceptThrottle, connectStreamId);
                break;
            }
        }

        private int storeRequest(final ListFW<HttpHeaderFW> headers, int requestCacheSlot)
        {
            // TODO, GC https://github.com/reaktivity/nukleus-maven-plugin/issues/16
            MutableDirectBuffer requestCacheBuffer = bufferPool.buffer(requestCacheSlot);
            headers.forEach(h ->
            {
                requestCacheBuffer.putBytes(this.requestSize, h.buffer(), h.offset(), h.sizeof());
                this.requestSize += h.sizeof();
            });
            return this.requestSize;
        }

        private boolean hasOutstandingRequestThatMaySatisfy(
            ListFW<HttpHeaderFW> requestHeaders,
            int requestURLHash)
        {
            if (urlToPendingStream.containsKey(requestURLHash))
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


        private void handleData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void afterBegin(
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
//          TODO
//            case AbortFW.TYPE_ID:
//                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
//                handleAbort(abort);
//                break;
            default:
                writer.doReset(acceptThrottle, connectStreamId);
                break;
            }
        }

        private void handleEnd(
                EndFW end)
        {
            writer.doHttpEnd(connect, connectStreamId);
        }

        private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(acceptThrottle, connectStreamId, bytes, frames);
        }

        private void handleReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, connectStreamId);
        }

        public void setReplyThrottle(GroupThrottle replyThrottle)
        {
            this.groupThrottle = replyThrottle;
        }

        public MessageConsumer replyMessageHandler()
        {
            return this.responseMessageHandler;
        }
    }


    private final class ProxyConnectReplyStream
    {
        private MessageConsumer streamState;

        private final MessageConsumer connectThrottle;
        private final long connectReplyStreamId;

        private Correlation streamCorrelation;
        private MessageConsumer acceptReply;
        private long acceptReplyStreamId;

        private long connectCorrelationId;

        private String acceptReplyName;

        private long acceptCorrelationId;

        private List<ProxyAcceptStream> forwardResponsesTo;

        private GroupThrottle replyThrottle;

        private Int2ObjectHashMap<List<ProxyAcceptStream>> awaitingRequestMatches;

        private ProxyConnectReplyStream(
                MessageConsumer connectReplyThrottle,
                long connectReplyId)
        {
            this.connectThrottle = connectReplyThrottle;
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
                writer.doReset(connectThrottle, connectReplyStreamId);
            }
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (forwardResponsesTo != null)
            {
                this.forwardResponsesTo.stream().forEach(
                    h -> h.replyMessageHandler().accept(msgTypeId, buffer, index, length)
                );
            }
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
//                  TODO
//                  case AbortFW.TYPE_ID:
//                      final AbortFW abort = abortRO.wrap(buffer, index, index + length);
//                      handleAbort(abort);
//                      break;
                default:
                    writer.doReset(connectThrottle, connectReplyStreamId);
                    break;
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            this.connectCorrelationId = begin.correlationId();
            streamCorrelation = connectRef == 0L ? correlations.remove(connectCorrelationId) : null;

            if (streamCorrelation != null)
            {
                this.acceptReplyName = streamCorrelation.acceptName();
                this.acceptReply = router.supplyTarget(acceptReplyName);
                this.acceptReplyStreamId = supplyStreamId.getAsLong();
                this.acceptCorrelationId = streamCorrelation.acceptCorrelation();
                this.awaitingRequestMatches = streamCorrelation.awaitingRequestMatches();

                int requestURLHash = streamCorrelation.requestURLHash();
                this.forwardResponsesTo = awaitingRequestMatches.remove(requestURLHash);
                if (requestURLHash == -1 || forwardResponsesTo == null)
                {
                    forwardResponsesTo = emptyList();
                }

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);

                // What if this stream is RESET?
                writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                    httpBeginEx.headers().forEach(h ->
                       e.item(h2 -> h2.representation((byte)0)
                                      .name(h.name().asString())
                                      .value(h.value().asString()))
                    )
                );
                this.replyThrottle = new GroupThrottle(forwardResponsesTo.size() +1,
                        writer,
                        connectThrottle,
                        connectReplyStreamId);

                router.setThrottle(acceptReplyName, acceptReplyStreamId, this::handleThrottle);
                this.forwardResponsesTo.stream().forEach(acceptStream ->
                {
                    acceptStream.setReplyThrottle(replyThrottle);
                    acceptStream.replyMessageHandler().accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.limit());
                });

                streamState = this::afterBegin;
            }
            else
            {
                writer.doReset(connectThrottle, connectReplyStreamId);
            }
        }

        private void handleData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void handleEnd(
            EndFW end)
        {
            writer.doHttpEnd(acceptReply, acceptReplyStreamId);
        }


        private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    this.replyThrottle.processWindow(window.streamId(), window.update());
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    this.replyThrottle.processReset(reset.streamId());
                    break;
                default:
                    // ignore
                    break;
            }
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