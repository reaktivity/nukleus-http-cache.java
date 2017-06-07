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

import static org.reaktivity.nukleus.http_cache.internal.routable.Routable.NOT_PRESENT;
import static org.reaktivity.nukleus.http_cache.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.hashRequestURL;

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
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http_cache.internal.routable.Route;
import org.reaktivity.nukleus.http_cache.internal.routable.Source;
import org.reaktivity.nukleus.http_cache.internal.routable.Target;
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
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> cachedRequestHeadersRO = new HttpBeginExFW().headers();
    private final HttpBeginExFW cachedResponseRO = new HttpBeginExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final Function<String, Target> supplyTargetRoute;
    private final LongObjectBiConsumer<Correlation> correlateNew;

    public final Int2IntHashMap urlToResponse;
    public final Int2IntHashMap urlToRequestHeaders;
    public final Int2IntHashMap urlToResponseLimit;
    public final Int2IntHashMap urlToRequestHeadersLimit;

    private final Slab slab;

    private final Int2ObjectHashMap<List<MessageHandler>> awaitingRequestMatches;

    public ProxyAcceptStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        LongObjectBiConsumer<Correlation> correlateNew,
        Function<String, Target> supplyTargetRoute,
        Int2IntHashMap urlToResponses,
        Int2IntHashMap urlToRequestHeaders,
        Int2IntHashMap urlToResponseLimit,
        Int2IntHashMap urlToRequestHeadersLimit,
        Int2ObjectHashMap<List<MessageHandler>> awaitingRequestMatches,
        Slab slab
       )
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.correlateNew = correlateNew;
        this.supplyTargetRoute = supplyTargetRoute;
        this.urlToResponse = urlToResponses;
        this.urlToRequestHeaders = urlToRequestHeaders;
        this.urlToResponseLimit = urlToResponseLimit;
        this.urlToRequestHeadersLimit = urlToRequestHeadersLimit;
        this.awaitingRequestMatches = awaitingRequestMatches;
        this.slab = slab;
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {

        private MessageHandler streamState;

        private long sourceId;
        private Target target;
        private long targetId;

        private Target replyTarget;
        private long correlationId;
        private String sourceName;
        // these are fields do to effectively final forEach
        private int requestSlabSize = 0;

        private List<MessageHandler> awaitingRequests;

        private MessageHandler addedMessageHandler;

        private String targetName;

        private long targetRef;

        private int requestCacheSlot;

        private boolean useAwaitedRequest = true;

        private int requestURLHash;

        private String requestURL;

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
                this.requestURLHash = hashRequestURL(requestURL);

                if(!canBeServedByCache(headers))
                {
                    this.targetId = proxyRequest(correlationId, targetName, targetRef,
                            httpBeginEx.headers());
                    this.streamState = this::afterBeginOrData;
                }
                else if(hasStoredResponseThatSatisfies(requestURL, requestURLHash, headers))
                {
                    // TODO
                }
                else if(hasOutstandingRequestThatMaySatisfy(headers))
                {
                    this.awaitingRequests =
                            awaitingRequestMatches.getOrDefault(requestURLHash, new ArrayList<MessageHandler>());
                    awaitingRequestMatches.put(requestURLHash, awaitingRequests);
                    this.requestCacheSlot = slab.acquire(newSourceId);
                    storeRequest(headers, requestCacheSlot);
                    this.addedMessageHandler = this::handleReply;
                    awaitingRequests.add(this.addedMessageHandler);
                    this.streamState = this::waitingForOutstanding;
                }
                else
                {
                    if(urlToRequestHeaders.get(requestURLHash) == NOT_PRESENT)
                    {
                        this.requestCacheSlot = slab.acquire(requestURLHash);
                        storeRequest(headers, requestCacheSlot);
                        urlToRequestHeaders.put(requestURLHash, requestCacheSlot);
                        urlToRequestHeadersLimit.put(requestURLHash, this.requestSlabSize);
                    }
                    this.targetId = proxyRequest(correlationId, targetName,
                            targetRef, httpBeginEx.headers());
                    this.streamState = this::afterBeginOrData;
                }
                this.sourceId = newSourceId;
            }

        }

        private int storeRequest(final ListFW<HttpHeaderFW> headers, int requestCacheSlot)
        {
            MutableDirectBuffer requestCacheBuffer = slab.buffer(requestCacheSlot);
            headers.forEach(h ->
            {
                requestCacheBuffer.putBytes(this.requestSlabSize, h.buffer(), h.offset(), h.sizeof());
                this.requestSlabSize += h.sizeof();
            });
            // TODO, make method static when this.requestCacheBufferSize is not needed (i.e. when having streaming API)
            // https://github.com/reaktivity/nukleus-maven-plugin/issues/16
            return this.requestSlabSize;
        }

        private long proxyRequest(
                final long correlationId,
                final String targetName,
                final long targetRef,
                final ListFW<HttpHeaderFW> headers)
        {
            final Target newTarget = supplyTargetRoute.apply(targetName);
            final long newTargetId = supplyTargetId.getAsLong();
            final long targetCorrelationId = newTargetId;
            final Correlation correlation = new Correlation(correlationId, source.routableName(),
                                                OUTPUT_ESTABLISHED, requestURL, requestURLHash);
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
            // NOTE: we never store anything right now so this always returns false
            // TODO remove if expired!!
            if(requestHeaders.anyMatch(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                return "cache-control".equals(name) && value.contains("no-cache");
            }))
            {
                return false;
            }

            final int responseSlot = urlToResponse.get(requestURLHash);
            final int requestSlot = urlToRequestHeaders.get(requestURLHash);
            if(responseSlot != NOT_PRESENT && requestSlot != NOT_PRESENT)
            {
                final MutableDirectBuffer requestBuffer = slab.buffer(requestSlot);
                final int cachedRequestLimit = urlToRequestHeadersLimit.get(requestURLHash);
                final MutableDirectBuffer responseBuffer = slab.buffer(responseSlot);
                final int responseLimit = urlToResponseLimit.get(requestURLHash);
                cachedRequestHeadersRO.wrap(requestBuffer, 0, cachedRequestLimit);
                cachedResponseRO.wrap(responseBuffer, 0, responseLimit);
                // is same url, else cache miss
                final String cachedRequestUrl = HttpHeadersUtil.getRequestURL(cachedRequestHeadersRO);
                if(cachedRequestUrl.equals(requestURL))
                {
                    return satisfiesVaries(cachedRequestHeadersRO, cachedResponseRO, requestHeaders);
                }

            }
            return false;
        }

        private boolean hasOutstandingRequestThatMaySatisfy(
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            final int requestSlot = urlToRequestHeaders.get(requestURLHash);
            // TODO optimize with expected varies
            final boolean hasXHttpCacheSync = requestHeaders.anyMatch(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                return "x-http-cache-sync".equals(name) && "always".equals(value);
            });
            boolean hasMatchingRequest = !(requestSlot == NOT_PRESENT);
            return hasMatchingRequest && hasXHttpCacheSync;
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
            if(useAwaitedRequest)
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
            String vary = HttpHeadersUtil.getHeader(responseHeaders, "vary");
            String myAuthorizationHeader = getHeader(getMyRequestHeaders(), "authorization");

            int cachedRequestSlot = urlToRequestHeaders.get(requestURLHash);
            int cachedRequestLimit = urlToRequestHeadersLimit.get(requestURLHash);
            ListFW<HttpHeaderFW> cachedHeaders =
                    cachedRequestHeadersRO.wrap(slab.buffer(cachedRequestSlot), 0, cachedRequestLimit);
            String cachedAuthorizationHeader = getHeader(cachedHeaders, "authorization");
            String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, "cache-control");
            boolean useSharedResponse = true;
            if(cacheControl != null && cacheControl.contains("public"))
            {
                useSharedResponse = true;
            }
            else if (cacheControl != null && cacheControl.contains("private"))
            {
                useSharedResponse = false;
            }
            else if(myAuthorizationHeader != null || cachedAuthorizationHeader != null)
            {
                useSharedResponse = false;
            }
            else if(vary != null)
            {
                useSharedResponse = Arrays.stream(vary.split("\\s*,\\s*")).anyMatch(v ->
                {
                    String cachedHeaderValue = HttpHeadersUtil.getHeader(cachedHeaders, v);
                    String myHeaderValue = HttpHeadersUtil.getHeader(getMyRequestHeaders(), v);
                    return Objects.equals(cachedHeaderValue, myHeaderValue);
                });
            }

            if(useSharedResponse)
            {
                this.replyTarget.doHttpBegin(this.sourceId, 0L, this.correlationId, e ->
                {
                    responseHeaders.forEach(h ->
                    {
                        e.item(h2 -> h2.representation((byte)0).name(h.name().asString()).value(h.value().asString()));
                    });
                });
            }
            else
            {
                this.useAwaitedRequest = false;
                ListFW<HttpHeaderFW> myRequestHeaders = getMyRequestHeaders();
                this.targetId = proxyRequest(correlationId, targetName, targetRef,
                            myRequestHeaders);
                this.target.doHttpEnd(this.targetId);
            }
        }

        private ListFW<HttpHeaderFW> getMyRequestHeaders()
        {
            MutableDirectBuffer myStoredRequest = slab.buffer(this.requestCacheSlot);
            ListFW<HttpHeaderFW> myRequestHeaders = cachedRequestHeadersRO
                    .wrap(myStoredRequest, 0, this.requestSlabSize);
            return myRequestHeaders;
        }

        private void processDataReply(
                DirectBuffer buffer,
                int index,
                int length)
        {
            dataRO.wrap(buffer, index, index + length);
            this.replyTarget.doHttpData(this.sourceId, dataRO.payload());
        }

        private void processEndReply(
            DirectBuffer buffer,
            int index,
            int length)
        {
            this.replyTarget.doHttpEnd(this.sourceId);
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

    private static boolean satisfiesVaries(
            ListFW<HttpHeaderFW> cachedRequestHeadersRO,
            HttpBeginExFW cachedResponseRO,
            ListFW<HttpHeaderFW> requestHeaders)
    {
        // TODO with out list when have streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        List<String> variesHeaders = new ArrayList<String>();
        cachedResponseRO.headers().forEach(h ->
        {
            if("vary".equals(h.name().asString()))
            {
                variesHeaders.addAll(Arrays.asList(h.value().asString().split("\\s*(,)\\s*")));
            }
        });
        return variesHeaders.stream().allMatch(v ->
        {
            if(cachedRequestHeadersRO.anyMatch(h -> v.equals(h.name().asString())))
            {
                String cachedHeader = getHeader(cachedRequestHeadersRO, v);
                if (cachedHeader == null)
                {
                    return requestHeaders.anyMatch(h -> v.equals(h.name().asString()));
                }
                else
                {
                   String requestHeader = getHeader(requestHeaders, v);
                   return cachedHeader.equals(requestHeader);
                }
            }
            else
            {
                return true;
            }
        });
    }
}
