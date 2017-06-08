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
package org.reaktivity.nukleus.http_cache.internal.routable.stream;

import static org.reaktivity.nukleus.http_cache.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.getRequestURL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http_cache.internal.routable.Route;
import org.reaktivity.nukleus.http_cache.internal.routable.Source;
import org.reaktivity.nukleus.http_cache.internal.routable.Target;
import org.reaktivity.nukleus.http_cache.internal.routable.stream.ProxyConnectReplyStreamFactory.GroupThrottle;
import org.reaktivity.nukleus.http_cache.internal.router.Correlation;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.util.function.LongObjectBiConsumer;

public final class ProxyAcceptStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW(); // sometimes used for request, sometimes response
    private final ListFW<HttpHeaderFW> headersRO = new HttpBeginExFW().headers();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final Function<String, Target> supplyTargetRoute;
    private final LongObjectBiConsumer<Correlation> correlateNew;

    public final Int2ObjectHashMap<SourceInputStream> urlToPendingStream;
    private final Slab slab;
    private final Int2ObjectHashMap<List<SourceInputStream>> awaitingRequestMatches;

    public ProxyAcceptStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        LongObjectBiConsumer<Correlation> correlateNew,
        Function<String, Target> supplyTargetRoute,
        Int2ObjectHashMap<SourceInputStream> urlToPendingStream,
        Int2ObjectHashMap<List<SourceInputStream>> awaitingRequestMatches,
        Slab slab
       )
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.correlateNew = correlateNew;
        this.supplyTargetRoute = supplyTargetRoute;
        this.awaitingRequestMatches = awaitingRequestMatches;
        this.urlToPendingStream = urlToPendingStream;
        this.slab = slab;
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    public final class SourceInputStream
    {

        private MessageHandler streamState;

        private long sourceId;
        private Target target;
        private long targetId;

        private Target replyTarget;
        private long correlationId;
        private String sourceName;

        private List<SourceInputStream> awaitingRequests;
        private String targetName;
        private long targetRef;
        private int requestCacheSlot;
        private boolean usePendingRequest = true;
        private int requestURLHash;
        private String requestURL;
        private MessageHandler replyMessageHandler;
        private GroupThrottle replyThrottle;

        // these are fields do to effectively final forEach
        private int requestSlabSize = 0;

        private SourceInputStream()
        {
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void afterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            case DataFW.TYPE_ID:
                // TODO forward/proxy data!
                long streamId = dataRO.wrap(buffer, index, length).streamId();
                source.doReset(streamId);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
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
                this.streamState = this::afterEnd;
                break;
            case DataFW.TYPE_ID:
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void afterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void afterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::afterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

            source.doReset(streamId);

            this.streamState = this::afterReplyOrReset;
        }


        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);

            final long newSourceId = begin.streamId();
            final long sourceRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            this.sourceName = begin.source().asString();

            this.correlationId = correlationId;
            source.doWindow(newSourceId, 0);

            final Optional<Route> optional = resolveTarget(sourceRef);

            if (optional.isPresent())
            {
                final Route route = optional.get();
                this.targetName = route.targetName();
                this.targetRef = route.targetRef();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> headers = httpBeginExRO.headers();
                this.requestURL = getRequestURL(headers);
                this.requestURLHash = requestURL.hashCode();

                if(!canBeServedByCache(headers))
                {
                    this.targetId = proxyRequest(correlationId, targetName, targetRef,
                            httpBeginEx.headers(), false);
                    clean();
                    this.streamState = this::afterBeginOrData;
                }
                else if(hasStoredResponseThatSatisfies(requestURL, requestURLHash, headers))
                {
                    // always false right now, so can't get here
                }
                else if(hasOutstandingRequestThatMaySatisfy(headers))
                {
                    this.awaitingRequests =
                            awaitingRequestMatches.getOrDefault(requestURLHash, new ArrayList<SourceInputStream>());
                    awaitingRequestMatches.put(requestURLHash, awaitingRequests);
                    this.requestCacheSlot = slab.acquire(newSourceId);
                    storeRequest(headers, requestCacheSlot);
                    this.replyMessageHandler = this::handleReply;
                    awaitingRequests.add(this);
                    this.streamState = this::waitingForOutstanding;
                }
                else
                {
                    boolean canBeUsedForPendingStream = !urlToPendingStream.containsKey(requestURLHash);
                    if(canBeUsedForPendingStream)
                    {
                        this.requestCacheSlot = slab.acquire(requestURLHash);
                        storeRequest(headers, requestCacheSlot);
                        urlToPendingStream.put(requestURLHash, this);
                    }
                    this.targetId = proxyRequest(correlationId, targetName,
                            targetRef, httpBeginEx.headers(), canBeUsedForPendingStream);
                    this.streamState = this::afterBeginOrData;
                }
                this.sourceId = newSourceId;
            }

        }

        private int storeRequest(final ListFW<HttpHeaderFW> headers, int requestCacheSlot)
        {
            // TODO, make method static when this.requestCacheBufferSize is not needed (i.e. when having streaming API)
            // https://github.com/reaktivity/nukleus-maven-plugin/issues/16
            MutableDirectBuffer requestCacheBuffer = slab.buffer(requestCacheSlot);
            headers.forEach(h ->
            {
                requestCacheBuffer.putBytes(this.requestSlabSize, h.buffer(), h.offset(), h.sizeof());
                this.requestSlabSize += h.sizeof();
            });
            return this.requestSlabSize;
        }

        private long proxyRequest(
                final long correlationId,
                final String targetName,
                final long targetRef,
                final ListFW<HttpHeaderFW> headers,
                boolean canBeUsedForCache)
        {
            final Target newTarget = supplyTargetRoute.apply(targetName);
            final long newTargetId = supplyTargetId.getAsLong();
            final long targetCorrelationId = newTargetId;
            // TODO, consider passing boolean in correlation, rather than magic value
            final Correlation correlation = canBeUsedForCache ?
                new Correlation(correlationId, source.routableName(), OUTPUT_ESTABLISHED, requestURLHash) :
                new Correlation(correlationId, source.routableName(), OUTPUT_ESTABLISHED, -1);
            correlateNew.accept(targetCorrelationId, correlation);
            newTarget.doHttpBegin(newTargetId, targetRef, targetCorrelationId, e ->
            {
                headers.forEach(h ->
                {
                    e.item(h2 -> h2.name(h.name().asString()).value(h.value().asString()));
                });
            });
            newTarget.addThrottle(newTargetId, this::handleThrottle);
            this.target = newTarget;
            return newTargetId;
        }

        private boolean hasStoredResponseThatSatisfies(
                final String requestURL,
                final int requestURLHash,
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            // NOTE: we never store anything right now so this always returns
            // false
            return false;
        }

        private boolean hasOutstandingRequestThatMaySatisfy(
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            if(urlToPendingStream.containsKey(requestURLHash))
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

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            this.target.doHttpEnd(this.targetId);
            final long streamId = endRO.streamId();
            this.streamState = this::afterEnd;

            source.removeStream(streamId);
            target.removeThrottle(targetId);
        }

        private Optional<Route> resolveTarget(
            long sourceRef)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);

            return routes.stream().findFirst();
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
                processWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            final int update = windowRO.update();

            source.doWindow(sourceId, update);
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

            source.doReset(sourceId);
        }

        private void handleReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if(usePendingRequest)
            {
                switch (msgTypeId)
                {
                    case BeginFW.TYPE_ID:
                        this.replyTarget = supplyTargetRoute.apply(this.sourceName);
                        processBeginReply(buffer, index, length);
                        break;
                    case DataFW.TYPE_ID:
                        processDataReply(buffer, index, length);
                        break;
                    case EndFW.TYPE_ID:
                        processEndReply(buffer, index, length);
                        break;
                    default:
                        // ignore
                        break;
                }
            }
        }

        private void processBeginReply(
                DirectBuffer buffer,
                int index,
                int length)
        {

            beginRO.wrap(buffer, index, index + length);
            final OctetsFW extension = beginRO.extension();
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();

            final String vary = HttpHeadersUtil.getHeader(responseHeaders, "vary");
            final String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, "cache-control");

            final SourceInputStream pendingRequest = urlToPendingStream.get(requestURLHash);

            pendingRequest.getRequestHeaders(headersRO);
            final String pendingRequestAuthorizationHeader = getHeader(headersRO, "authorization");

            getRequestHeaders(headersRO);
            final String myAuthorizationHeader = getHeader(headersRO, "authorization");

            boolean useSharedResponse = true;

            if(cacheControl != null && cacheControl.contains("public"))
            {
                useSharedResponse = true;
            }
            else if (cacheControl != null && cacheControl.contains("private"))
            {
                useSharedResponse = false;
            }
            else if(myAuthorizationHeader != null || pendingRequestAuthorizationHeader != null)
            {
                useSharedResponse = false;
            }
            else if(vary != null)
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

            if(useSharedResponse)
            {
                clean();
                this.targetId = supplyTargetId.getAsLong();
                this.replyTarget.doHttpBegin(targetId, 0L, this.correlationId, e ->
                {
                    responseHeaders.forEach(h ->
                    {
                        e.item(h2 -> h2.representation((byte)0).name(h.name().asString()).value(h.value().asString()));
                    });
                });
                this.replyTarget.addThrottle(this.targetId, this.replyThrottle::handleThrottle);
            }
            else
            {
                this.replyThrottle.optOut();
                this.usePendingRequest = false;
                // TODO, consider allowing use in next cache, timing on clearing out stored
                // request is needed for this to work though
                getRequestHeaders(headersRO);
                this.targetId = proxyRequest(correlationId, targetName, targetRef,
                        headersRO, false);
                clean();
                this.target.doHttpEnd(this.targetId);
            }
        }

        private ListFW<HttpHeaderFW> getRequestHeaders(final ListFW<HttpHeaderFW> headersFW)
        {
            return headersFW.wrap(slab.buffer(this.requestCacheSlot), 0, this.requestSlabSize);
        }

        private void processDataReply(
                DirectBuffer buffer,
                int index,
                int length)
        {
            dataRO.wrap(buffer, index, index + length);
            this.replyTarget.doHttpData(this.targetId, dataRO.payload());
        }

        private void processEndReply(
            DirectBuffer buffer,
            int index,
            int length)
        {
            this.replyTarget.doHttpEnd(this.targetId);
        }

        public void clean()
        {
            if(this.requestCacheSlot != NO_SLOT)
            {
                slab.release(this.requestCacheSlot);
            }
        }

        public MessageHandler replyMessageHandler()
        {
            return replyMessageHandler;
        }

        public void setReplyThrottle(GroupThrottle replyThrottle)
        {
            this.replyThrottle = replyThrottle;
        }

    }

    public static boolean canBeServedByCache(ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
                case "cache-control":
                    if(value.contains("no-cache"))
                    {
                        return false;
                    }
                    return true;
                case ":method":
                    if("GET".equalsIgnoreCase(value))
                    {
                        return false;
                    }
                    return true;
                default:
                    return false;
                }
        });
    }
}
