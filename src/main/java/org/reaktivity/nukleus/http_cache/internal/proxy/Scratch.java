package org.reaktivity.nukleus.http_cache.internal.proxy;

//import static java.lang.Integer.parseInt;
//import static java.lang.System.currentTimeMillis;
//import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
//import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.canInjectPushPromise;
//import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isPrivatelyCacheable;
//import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl.getMaxAgeFreshnessExtension;
//import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
//
//import java.util.function.Consumer;
//
//import org.agrona.DirectBuffer;
//import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
//import org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader;
//import org.reaktivity.nukleus.http_cache.internal.stream.ProxyStreamFactory;
//import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
//import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
//import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
//import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
//import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
//import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
//import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
//import org.reaktivity.nukleus.http_cache.internal.types.ListFW.Builder;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
//import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Scratch
{

//    private void sendHttpResponse(
//            final ListFW<HttpHeaderFW> responseHeaders,
//            final ListFW<HttpHeaderFW> requestHeaders)
//    {
//        if (isPrivatelyCacheable(responseHeaders)
//                && requestHeaders.anyMatch(PreferHeader.PREFER_RESPONSE_WHEN_MODIFIED)
//                && canInjectPushPromise(requestHeaders))
//        {
//
//            if (responseHeaders.anyMatch(h -> HttpHeaders.CACHE_CONTROL.equals(h.name().asString())))
//            {
//                streamFactory.writer.doHttpBegin2(acceptReply, acceptReplyStreamId, 0L,
//                        acceptCorrelationId, appendStaleWhileRevalidate(responseHeaders));
//            }
//            else
//            {
//                streamFactory.writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L,
//                        acceptCorrelationId, injectStaleWhileRevalidate(headersToExtensions(responseHeaders)));
//            }
//            injectPushPromise(requestHeaders, responseHeaders);
//        }
//        else
//        {
//            streamFactory.writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
//            responseHeaders.forEach(h -> e.item(
//                    h2 -> h2.representation((byte) 0)
//                    .name(h.name())
//                    .value(h.value())))
//                    );
//        }
//    }

//    private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> appendStaleWhileRevalidate(
//            ListFW<HttpHeaderFW> headersFW)
//    {
//        return x -> headersFW
//                .forEach(h ->
//                    {
//                        final StringFW nameRO = h.name();
//                        final String16FW valueRO = h.value();
//                        final String name = nameRO.asString();
//                        final String value = valueRO.asString();
//                        if (HttpHeaders.CACHE_CONTROL.equals(name) && !value.contains(ProxyStreamFactory.STALE_WHILE_REVALIDATE_2147483648))
//                        {
//                            x.item(y -> y.representation((byte) 0)
//                                    .name(nameRO)
//                                    .value(value + ", " + ProxyStreamFactory.STALE_WHILE_REVALIDATE_2147483648));
//                        }
//                        else
//                        {
//                            x.item(y -> y.representation((byte) 0).name(nameRO).value(h.value()));
//                        }
//                    }
//                );
//    }
//
//    private Flyweight.Builder.Visitor injectStaleWhileRevalidate(
//        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
//    {
//        mutator = mutator.andThen(
//                x ->  x.item(h -> h.representation((byte) 0).name("cache-control").value(ProxyStreamFactory.STALE_WHILE_REVALIDATE_2147483648))
//            );
//        return visitHttpBeginEx(mutator);
//    }
//
//    private Flyweight.Builder.Visitor visitHttpBeginEx(
//            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
//
//    {
//        return (buffer, offset, limit) ->
//        streamFactory.httpBeginExRW.wrap(buffer, offset, limit)
//                     .headers(headers)
//                     .build()
//                     .sizeof();
//    }
//
//    private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headersToExtensions(
//            ListFW<HttpHeaderFW> headersFW)
//    {
//        return x -> headersFW
//                .forEach(h ->
//                x.item(y -> y.representation((byte) 0)
//                        .name(h.name())
//                        .value(h.value())));
//    }
//
//    private void injectPushPromise(
//        ListFW<HttpHeaderFW> requestHeaders,
//        ListFW<HttpHeaderFW> responseHeaders)
//    {
//        streamFactory.writer.doH2PushPromise(
//                acceptReply,
//                acceptReplyStreamId,
//                setPushPromiseHeaders(requestHeaders, responseHeaders));
//          }
//    

//    private void fanout(
//            final ListFW<HttpHeaderFW> requestHeaders,
//            final boolean follow304)
//        {
//            // create connection
//            this.requestSlot = streamFactory.streamBufferPool.acquire(acceptStreamId);
//            if (requestSlot == NO_SLOT)
//            {
//                send503AndReset();
//                return;
//            }
//            storeRequest(requestHeaders, requestSlot);
//
//            int correlationRequestHeadersSlot = streamFactory.streamBufferPool.acquire(requestURLHash);
//            if (correlationRequestHeadersSlot == NO_SLOT)
//            {
//                streamFactory.streamBufferPool.release(requestSlot);
//                send503AndReset();
//                return;
//            }
//
//            this.connectStreamId = streamFactory.supplyStreamId.getAsLong();
//            storeRequest(requestHeaders, correlationRequestHeadersSlot);
//
//            this.junction = streamFactory.new FanOut();
//
//            Request correlation = new Request(
//                requestURLHash,
//                junction,
//                streamFactory.correlationBufferPool,
//                correlationRequestHeadersSlot,
//                requestSize,
//                follow304,
//                connectName,
//                connectRef);
//
//            streamFactory.correlations.put(connectCorrelationId, correlation);
//
//            junction.setStreamCorrelation(correlation);
//            ProxyStreamFactory.long2ObjectPutIfAbsent(streamFactory.junctions, requestURLHash, junction);
//            junction.addSubscriber(this::handleResponseFromMyInitiatedFanout);
//
//            String wait = PreferHeader.getWait(requestHeaders, streamFactory.cacheControlParser);
//
//            if (wait != null)
//            {
//                streamFactory.scheduler.accept(currentTimeMillis() + parseInt(wait) * 1000, () ->
//                {
//                    if (!junction.getOuts().isEmpty())
//                    {
//                        final ListFW<HttpHeaderFW> scheduledRequestHeaders = getRequestHeaders(streamFactory.requestHeadersRO);
//                        streamFactory.sendRequest(connect, connectStreamId, connectRef, connectCorrelationId, scheduledRequestHeaders);
//                    }
//                    else
//                    {
//                        streamFactory.correlations.remove(connectCorrelationId).cleanUp();
//                    }
//                });
//            }
//            else
//            {
//                streamFactory.sendRequest(connect, connectStreamId, connectRef, connectCorrelationId, requestHeaders);
//            }
//
//            this.streamState = this::waitingForOutstandingResponseFromMyInitiatedFanout;
//        }
//
//        private void latchOnToFanout(final ListFW<HttpHeaderFW> requestHeaders)
//        {
//            this.requestSlot = streamFactory.streamBufferPool.acquire(acceptStreamId);
//            if (requestSlot == NO_SLOT)
//            {
//                send503AndReset();
//                return;
//            }
//            storeRequest(requestHeaders, requestSlot);
//
//            this.junction = streamFactory.junctions.get(requestURLHash);
//            junction.addSubscriber(this::handleResponseFromFanout);
//
//            // send 0 window back to complete handshake
//
//            this.streamState = this::waitingForOutstandingResponseFromLatchedOnFanout;
//        }

//    private void waitingForOutstanding(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//    {
//        switch (msgTypeId)
//        {
//        case EndFW.TYPE_ID:
//            // TODO H2 late headers?? RFC might say can't affect caching, but
//            // probably should still forward should expected request not match...
//            break;
//        case AbortFW.TYPE_ID:
//            if (junction != null)
//            {
//                junction.unsubscribe(this::handleResponseFromMyInitiatedFanout);
//            }
//            break;
//        case DataFW.TYPE_ID:
//        default:
//            if (junction != null)
//            {
//                // needed because could be handleMyInitatedFanOut OR handleFanout
//                //  it could already be processed in which case this doesn't mean
//                // much...
//                junction.unsubscribe(this::handleResponseFromMyInitiatedFanout);
//            }
//            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
//            break;
//        }
//    }
//
//    private void waitingForOutstandingResponseFromLatchedOnFanout(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//    {
//        switch (msgTypeId)
//        {
//        case EndFW.TYPE_ID:
//            // TODO H2 late headers?? RFC might say can't affect caching, but
//            // probably should still forward should expected request not match...
//            break;
//        case AbortFW.TYPE_ID:
//            junction.unsubscribe(this::handleResponseFromFanout);
//            break;
//        case DataFW.TYPE_ID:
//        default:
//            junction.unsubscribe(this::handleResponseFromFanout);
//            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
//            break;
//        }
//    }
//
//    private void waitingForOutstandingResponseFromMyInitiatedFanout(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//    {
//        switch (msgTypeId)
//        {
//        case EndFW.TYPE_ID:
//            // TODO H2 late headers?? RFC might say can't affect caching, but
//            // probably should still forward should expected request not match...
//            break;
//        case AbortFW.TYPE_ID:
//            junction.unsubscribe(this::handleResponseFromMyInitiatedFanout);
//            break;
//        default:
//            junction.unsubscribe(this::handleResponseFromMyInitiatedFanout);
//            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
//            break;
//        }
//    }

//    private void handleResponseFromProxy(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//    {
//        switch (msgTypeId)
//        {
//            case BeginFW.TYPE_ID:
//                final BeginFW begin = streamFactory.beginRO.wrap(buffer, index, index + length);
//                final OctetsFW extension = begin.extension();
//                final HttpBeginExFW httpBeginEx = extension.get(streamFactory.httpBeginExRO::wrap);
//                final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
//                final ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders(streamFactory.requestHeadersRO);
//                sendHttpResponse(responseHeaders, requestHeaders);
//                this.connectReplyStreamId = request.getConnectReplyStreamId();
//                this.connectReplyThrottle = request.connectReplyThrottle();
//                streamFactory.router.setThrottle(acceptName, acceptReplyStreamId, this::handleAcceptReplyThrottle);
//
//                break;
//            default:
//                proxyBackAfterBegin(msgTypeId, buffer, index, length);
//                break;
//        }
//    }
//
//    private void proxyBackAfterBegin(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//    {
//        switch (msgTypeId)
//        {
//            case DataFW.TYPE_ID:
//                streamFactory.dataRO.wrap(buffer, index, index + length);
//                OctetsFW payload = streamFactory.dataRO.payload();
//                streamFactory.writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
//                break;
//            case EndFW.TYPE_ID:
//                streamFactory.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
//                clean();
//                break;
//            case AbortFW.TYPE_ID:
//                streamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
//                clean();
//                break;
//            default:
//                break;
//        }
//    }
//
//    private void clean()
//    {
//        if (this.requestSlot != NO_SLOT)
//        {
//            streamFactory.streamBufferPool.release(this.requestSlot);
//            this.requestSlot = NO_SLOT;
//        }
//    }
//
//    private ListFW<HttpHeaderFW> getRequestHeaders(ListFW<HttpHeaderFW> headersRO)
//    {
//        return headersRO.wrap(streamFactory.streamBufferPool.buffer(this.requestSlot), 0, requestSize);
//    }

/*
 *
   private void handleAcceptReplyThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
{
    switch (msgTypeId)
    {
        case WindowFW.TYPE_ID:
            final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
            handleAcceptReplyWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = streamFactory.resetRO.wrap(buffer, index, index + length);
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
    final int bytes = streamFactory.windowRO.update();
    final int frames = streamFactory.windowRO.frames();

    streamFactory.writer.doWindow(connectReplyThrottle, this.connectReplyStreamId, bytes, frames);
}

private void handleAcceptReplyReset(
    ResetFW reset)
{
    streamFactory.writer.doReset(connectReplyThrottle, this.connectReplyStreamId);
}
*/

//    package org.reaktivity.nukleus.http_cache.internal.stream;
//
//    import static java.lang.Integer.parseInt;
//    import static java.lang.System.currentTimeMillis;
//    import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
//    import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.isCacheable;
//    import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
//    import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
//    import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
//    import static org.reaktivity.nukleus.http_cache.internal.stream.util.PreferHeader.PREFER_RESPONSE_WHEN_MODIFIED;
//
//    import java.util.Objects;
//
//    import org.agrona.DirectBuffer;
//    import org.agrona.MutableDirectBuffer;
//    import org.reaktivity.nukleus.function.MessageConsumer;
//    import org.reaktivity.nukleus.http_cache.internal.proxy.Request;
//    import org.reaktivity.nukleus.http_cache.internal.stream.util.CacheEntry;
//    import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils;
//    import org.reaktivity.nukleus.http_cache.internal.stream.util.PreferHeader;
//    import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
//    import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
//
//    final class ProxyConnectReplyStream
//    {
//        private final ProxyStreamFactory proxyStreamFactory;
//
//        private MessageConsumer streamState;
//
//        private final MessageConsumer connectReplyThrottle;
//        private final long connectReplyStreamId;
//
//        private Request streamCorrelation;
//        private MessageConsumer acceptReply;
//
//        // For initial caching
//        private int cacheResponseSlot = NO_SLOT;
//        private int cacheResponseSize = 0;
//        private int cacheResponseHeadersSize = 0;
//
//        // For trying to match existing cache
//        private int cachedResponseSize;
//        private int processedResponseSize;
//        private CacheEntry cacheServer;
//
//        ProxyConnectReplyStream(
//                ProxyStreamFactory proxyStreamFactory, MessageConsumer connectReplyThrottle,
//                long connectReplyId)
//        {
//            this.proxyStreamFactory = proxyStreamFactory;
//            this.connectReplyThrottle = connectReplyThrottle;
//            this.connectReplyStreamId = connectReplyId;
//            this.streamState = this::beforeBegin;
//        }
//
//        void handleStream(
//                int msgTypeId,
//                DirectBuffer buffer,
//                int index,
//                int length)
//        {
//            streamState.accept(msgTypeId, buffer, index, length);
//        }
//
//        private void beforeBegin(
//                int msgTypeId,
//                DirectBuffer buffer,
//                int index,
//                int length)
//        {
//            if (msgTypeId == BeginFW.TYPE_ID)
//            {
//                final BeginFW begin = this.proxyStreamFactory.beginRO.wrap(buffer, index, index + length);
//                handleBegin(begin);
//            }
//            else
//            {
//                this.proxyStreamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
//            }
//        }
//
//        private void handleBegin(
//                BeginFW begin)
//        {
//            final long connectReplyRef = begin.sourceRef();
//            final long connectCorrelationId = begin.correlationId();
//
//            this.streamCorrelation = connectReplyRef == 0L ?
//                this.proxyStreamFactory.correlations.remove(connectCorrelationId) : null;
//
//            if (streamCorrelation != null)
//            {
//                if (streamCorrelation.follow304())
//                {
//                    final OctetsFW extension = begin.extension();
//                    final HttpBeginExFW httpBeginFW = extension.get(this.proxyStreamFactory.httpBeginExRO::wrap);
//                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
//                    final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(this.proxyStreamFactory.pendingRequestHeadersRO);
//                    if (requestHeaders.anyMatch(PREFER_RESPONSE_WHEN_MODIFIED) &&
//                        responseHeaders.anyMatch(h -> STATUS.equals(h.name().asString())
//                                            && "304".equals(h.value().asString())))
//                    {
//                        redoRequest(requestHeaders);
//                    }
//                    else
//                    {
//                        int requestURLHash = streamCorrelation.requestURLHash();
//                        if(this.proxyStreamFactory.cache.getCachedResponseThatSatisfies(requestURLHash, requestHeaders, true) != null)
//                        {
//                            this.cacheServer = this.proxyStreamFactory.cache.getCachedResponseThatSatisfies(requestURLHash, requestHeaders, true);
//                            forwardIfModified(begin, streamCorrelation, cacheServer, responseHeaders);
//                        }
//                        else
//                        {
//                            forwardBeginToAcceptReply(begin, streamCorrelation);
//                        }
//                    }
//                }
//                else
//                {
//                    forwardBeginToAcceptReply(begin, streamCorrelation);
//                }
//            }
//            else
//            {
//                this.proxyStreamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
//            }
//        }
//
//        private void redoRequest(
//                final ListFW<HttpHeaderFW> requestHeaders)
//        {
//            final String connectName = streamCorrelation.connectName();
//            final long connectRef = streamCorrelation.connectRef();
//            final MessageConsumer newConnect = this.proxyStreamFactory.router.supplyTarget(connectName);
//            final long newConnectStreamId = this.proxyStreamFactory.supplyStreamId.getAsLong();
//            final long newConnectCorrelationId = this.proxyStreamFactory.supplyCorrelationId.getAsLong();
//
//            String waitTime = PreferHeader.getWait(requestHeaders, this.proxyStreamFactory.cacheControlParser);
//            if (waitTime != null)
//            {
//                this.proxyStreamFactory.scheduler.accept(currentTimeMillis() + parseInt(waitTime) * 1000, () ->
//                    this.proxyStreamFactory.sendRequest(newConnect,
//                                newConnectStreamId,
//                                connectRef,
//                                newConnectCorrelationId,
//                                requestHeaders)
//                );
//            }
//            else
//            {
//                this.proxyStreamFactory.sendRequest(newConnect, newConnectStreamId, connectRef, newConnectCorrelationId, requestHeaders);
//            }
//            this.proxyStreamFactory.correlations.put(newConnectCorrelationId, streamCorrelation);
//            streamState = this::ignoreRest;
//        }
//
//        private void forwardBeginToAcceptReply(
//                final BeginFW begin,
//                final Request streamCorrelation)
//        {
//
//            streamCorrelation.connectReplyStreamId(connectReplyStreamId);
//            acceptReply = streamCorrelation.consumer();
//
//            final boolean requestCouldBeCached = streamCorrelation.requestSlot() != NO_SLOT;
//            if (requestCouldBeCached && cache(begin))
//            {
//                streamState = this::cacheAndForwardBeginToAcceptReply;
//                streamCorrelation.setConnectReplyThrottle((msgTypeId, buffer, index, length) ->
//                {
//                    switch(msgTypeId)
//                    {
//                        case ResetFW.TYPE_ID:
//                            // DPW TODO stop caching!
//                        default:
//                            break;
//                    }
//                    this.connectReplyThrottle.accept(msgTypeId, buffer, index, length);
//                });
//                this.acceptReply.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
//            }
//            else
//            {
//                streamState = acceptReply;
//                streamCorrelation.setConnectReplyThrottle(connectReplyThrottle);
//                acceptReply.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
//                if (requestCouldBeCached)
//                {
//                    streamCorrelation.cleanUp();
//                }
//            }
//
//        }
//
//        private void cacheAndForwardBeginToAcceptReply(
//            int msgTypeId,
//            DirectBuffer buffer,
//            int index,
//            int length)
//        {
//            acceptReply.accept(msgTypeId, buffer, index, length);
//            boolean continueCaching = true;
//                switch (msgTypeId)
//                {
//                    case DataFW.TYPE_ID:
//                        DataFW data = this.proxyStreamFactory.dataRO.wrap(buffer, index, index + length);
//                        continueCaching = cache(data);
//                        break;
//                    case EndFW.TYPE_ID:
//                        EndFW end = this.proxyStreamFactory.endRO.wrap(buffer, index, index + length);
//                        continueCaching = cache(end);
//                        break;
//                    case AbortFW.TYPE_ID:
//                        AbortFW abort = this.proxyStreamFactory.abortRO.wrap(buffer, index, index + length);
//                        continueCaching = cache(abort);
//                        break;
//                }
//            if (!continueCaching)
//            {
//                streamCorrelation.cleanUp();
//                streamState = this.acceptReply;
//            }
//        }
//
//        private void ignoreRest(int msgTypeId, DirectBuffer buffer, int index, int length)
//        {
//            // NOOP
//        }
//
//        private void forwardIfModified(
//                final BeginFW begin,
//                final Request streamCorrelation,
//                CacheEntry cacheServer,
//                ListFW<HttpHeaderFW> responseHeaders)
//        {
//            ListFW<HttpHeaderFW> cachedResponseHeaders = cacheServer.getResponseHeaders();
//            String cachedStatus = getHeader(cachedResponseHeaders, STATUS);
//            String status = getHeader(responseHeaders, STATUS);
//            String cachedContentLength = getHeader(cachedResponseHeaders, CONTENT_LENGTH);
//            String contentLength = getHeader(cachedResponseHeaders, CONTENT_LENGTH);
//            if (cachedStatus.equals(status) && Objects.equals(contentLength, cachedContentLength))
//            {
//                this.cacheServer = cacheServer;
//                cacheServer.addClient();
//                // store headers for in case if fails
//                this.cacheResponseSlot = this.proxyStreamFactory.streamBufferPool.acquire(streamCorrelation.requestURLHash());
//                cacheResponseHeaders(responseHeaders);
//
//                this.cachedResponseSize = cacheServer.getResponse(this.proxyStreamFactory.octetsRO).sizeof();
//                this.processedResponseSize = 0;
//                final int bytes = cachedResponseSize + 8024;
//                this.proxyStreamFactory.writer.doWindow(connectReplyThrottle, connectReplyStreamId, bytes, bytes);
//                streamState = this::attemptCacheMatch;
//            }
//            else
//            {
//                forwardBeginToAcceptReply(begin, streamCorrelation);
//            }
//        }
//
//        private void attemptCacheMatch(
//                int msgTypeId,
//                DirectBuffer buffer,
//                int index,
//                int length)
//        {
//            switch (msgTypeId)
//            {
//                case DataFW.TYPE_ID:
//                    final DataFW data = this.proxyStreamFactory.dataRO.wrap(buffer, index, index + length);
//                    final OctetsFW payload = data.payload();
//                    final int sizeofData = payload.sizeof();
//                    final OctetsFW cachedPayload = cacheServer.getResponse(this.proxyStreamFactory.octetsRO);
//                    boolean matches = (cachedResponseSize - processedResponseSize >= sizeofData) &&
//                    confirmMatch(payload, cachedPayload, processedResponseSize);
//                    if (!matches)
//                    {
//                        forwardHalfCachedResponse(payload);
//                    }
//                    else
//                    {
//                        this.processedResponseSize += sizeofData;
//                    }
//                    // consider removing window update when
//                    // https://github.com/reaktivity/k3po-nukleus-ext.java/issues/16
//                    break;
//                case EndFW.TYPE_ID:
//                    if (processedResponseSize == cachedResponseSize)
//                    {
//                        this.proxyStreamFactory.streamBufferPool.release(cacheResponseSlot);
//                        final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(this.proxyStreamFactory.pendingRequestHeadersRO);
//                        redoRequest(requestHeaders);
//                    }
//                    else
//                    {
//                        forwardCompletelyCachedResponse(buffer, index, length);
//                    }
//                    break;
//                case BeginFW.TYPE_ID:
//                case AbortFW.TYPE_ID:
//                default:
//                    //error cases// Forward 503?
//            }
//        }
//
//        private void forwardCompletelyCachedResponse(
//                DirectBuffer buffer,
//                int index,
//                int length)
//        {
//            MutableDirectBuffer storeResponseBuffer = this.proxyStreamFactory.cacheBufferPool.buffer(cacheResponseSlot);
//            storeResponseBuffer.putBytes(
//                    this.cacheResponseSize,
//                    cacheServer.getResponse(this.proxyStreamFactory.octetsRO).buffer(),
//                    0,
//                    processedResponseSize);
//            this.cachedResponseSize += processedResponseSize;
//            cache(this.proxyStreamFactory.endRO.wrap(buffer, index, index + length));
//            int requestURLHash = streamCorrelation.requestURLHash();
//            CacheEntry serverStream = this.proxyStreamFactory.cache.get(requestURLHash);
//            serverStream.forwardToClient(streamCorrelation);
//        }
//
//        private boolean confirmMatch(
//                OctetsFW payload,
//                OctetsFW cachedPayload,
//                int processedResponseSize)
//        {
//            final int payloadSizeOf = payload.sizeof();
//            final int payloadOffset = payload.offset();
//            final DirectBuffer payloadBuffer = payload.buffer();
//
//            final int cachedPayloadSizeOf = cachedPayload.sizeof();
//            final int cachedPayloadOffset = cachedPayload.offset();
//            final DirectBuffer cachedPayloadBuffer = cachedPayload.buffer();
//
//            if(payloadSizeOf <= cachedPayloadSizeOf)
//            {
//                for (int i = 0, length = payloadSizeOf; i < length; i++)
//                {
//                    if(cachedPayloadBuffer.getByte(cachedPayloadOffset + processedResponseSize + i)
//                            != payloadBuffer.getByte(payloadOffset + i))
//                    {
//                        return false;
//                    }
//                }
//                return true;
//            }
//            return false;
//        }
//
//        private void forwardHalfCachedResponse(
//                OctetsFW payload)
//        {
//            MutableDirectBuffer buffer = this.proxyStreamFactory.cacheBufferPool.buffer(cacheResponseSlot);
//
//            // copy overwhat has been matched
//            if(processedResponseSize > 0)
//            {
//                final OctetsFW cachedResponse = cacheServer.getResponse(this.proxyStreamFactory.octetsRO);
//                final DirectBuffer cachedBuffer = cachedResponse.buffer();
//                final int cachedOffset = this.proxyStreamFactory.octetsRO.offset();
//                buffer.putBytes(this.cacheResponseSize, cachedBuffer, cachedOffset, processedResponseSize);
//                this.cacheResponseSize += processedResponseSize;
//            }
//
//            // add new payload
//            int payloadSize = payload.sizeof();
//            buffer.putBytes(this.cacheResponseSize, payload.buffer(), payload.offset(), payloadSize);
//            this.cacheResponseSize += payloadSize;
//
//            streamState = this::waitForFullResponseThenForward;
//        }
//
//        private void waitForFullResponseThenForward(
//                int msgTypeId,
//                DirectBuffer buffer,
//                int index,
//                int length)
//        {
//            boolean cachedSuccessfully = true;
//            switch(msgTypeId)
//            {
//                case DataFW.TYPE_ID:
//                    final DataFW data = this.proxyStreamFactory.dataRO.wrap(buffer, index, index + length);
//                    cachedSuccessfully = cache(data);
//                    break;
//                case EndFW.TYPE_ID:
//                    EndFW end = this.proxyStreamFactory.endRO.wrap(buffer, index, index + length);
//                    cachedSuccessfully = cache(end);
//                    if (cachedSuccessfully)
//                    {
//                        int requstURLHash = streamCorrelation.requestURLHash();
//                        CacheEntry serverStream = this.proxyStreamFactory.cache.get(requstURLHash);
//                        serverStream.forwardToClient(streamCorrelation);
//                    }
//                    break;
//                case AbortFW.TYPE_ID:
//                default:
//                    // NOTE odd behavior if there is a cached Resource XYZ and then a
//                    // new one comes in that matches but appends, i.e. XYZAB AND XYZAB
//                    // size exceed cache capacity then it will 503...
//                    sendAbortOnCacheOOM();
//            }
//            if (!cachedSuccessfully)
//            {
//                sendAbortOnCacheOOM();
//            }
//        }
//
//        private void sendAbortOnCacheOOM()
//        {
//            long acceptReplyStreamId = this.proxyStreamFactory.supplyStreamId.getAsLong();
//            this.proxyStreamFactory.writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, 0L, e ->
//            e.item(h -> h.representation((byte) 0)
//                    .name(":status")
//                    .value("503")));
//            this.proxyStreamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
//        }
//
//        private boolean cache(BeginFW begin)
//        {
//            final OctetsFW extension = begin.extension();
//            final HttpBeginExFW httpBeginEx = extension.get(this.proxyStreamFactory.httpBeginExRO::wrap);
//            final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
//            final boolean isCacheable = isCacheable(responseHeaders);
//            final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(this.proxyStreamFactory.pendingRequestHeadersRO);
//            if (isCacheable && !requestHeaders.anyMatch(CacheUtils::isCacheControlNoStore))
//            {
//                this.cacheResponseSlot = this.proxyStreamFactory.cacheBufferPool.acquire(this.connectReplyStreamId);
//                if (cacheResponseSlot == NO_SLOT)
//                {
//                    return false;
//                }
//                int sizeof = responseHeaders.sizeof();
//                if (cacheResponseSize  + sizeof > this.proxyStreamFactory.cacheBufferPool.slotCapacity())
//                {
//                    this.proxyStreamFactory.cacheBufferPool.release(this.cacheResponseSlot);
//                    return false;
//                }
//                else
//                {
//                    cacheResponseHeaders(responseHeaders);
//                    return true;
//                }
//            }
//            else
//            {
//                return false;
//            }
//        }
//
//        private void cacheResponseHeaders(final ListFW<HttpHeaderFW> responseHeaders)
//        {
//            MutableDirectBuffer buffer = this.proxyStreamFactory.cacheBufferPool.buffer(this.cacheResponseSlot);
//            final int headersSize = responseHeaders.sizeof();
//            buffer.putBytes(cacheResponseSize, responseHeaders.buffer(), responseHeaders.offset(), headersSize);
//            cacheResponseSize += headersSize;
//            this.cacheResponseHeadersSize = headersSize;
//        }
//
//        private boolean cache(DataFW data)
//        {
//            OctetsFW payload = data.payload();
//            int sizeof = payload.sizeof();
//            if (cacheResponseSize + sizeof + 4 > this.proxyStreamFactory.cacheBufferPool.slotCapacity())
//            {
//                this.proxyStreamFactory.cacheBufferPool.release(this.cacheResponseSlot);
//                return false;
//            }
//            else
//            {
//                MutableDirectBuffer buffer = this.proxyStreamFactory.cacheBufferPool.buffer(this.cacheResponseSlot);
//                buffer.putBytes(cacheResponseSize, payload.buffer(), payload.offset(), sizeof);
//                cacheResponseSize += sizeof;
//                return true;
//            }
//        }
//
//        private boolean cache(EndFW end)
//        {
//            // TODO H2 end headers
//            final int requestURLHash = this.streamCorrelation.requestURLHash();
//            final int requestSlot = this.streamCorrelation.requestSlot();
//            final int requestSize = this.streamCorrelation.requestSize();
//            final int responseSlot = this.cacheResponseSlot;
//            final int responseHeadersSize = this.cacheResponseHeadersSize;
//            final int responseSize = this.cacheResponseSize;
//            this.proxyStreamFactory.cache.put(requestURLHash, requestSlot, requestSize, responseSlot, responseHeadersSize, responseSize);
//            return true;
//        }
//
//        private boolean cache(AbortFW abort)
//        {
//              this.proxyStreamFactory.cacheBufferPool.release(this.cacheResponseSlot);
//              return false;
//        }
//    }
}
