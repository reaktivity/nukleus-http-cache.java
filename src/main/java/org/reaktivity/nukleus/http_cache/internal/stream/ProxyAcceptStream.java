package org.reaktivity.nukleus.http_cache.internal.stream;

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.canBeServedByCache;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.PREFER_RESPONSE_WHEN_MODIFIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheEntry;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnModification;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.ProxyRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
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

final class ProxyAcceptStream
{
    private final ProxyStreamFactory streamFactory;
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

    private int requestSlot = NO_SLOT;
    private int requestSize;
    private Request request;
    private int requestURLHash;

    ProxyAcceptStream(
            ProxyStreamFactory streamFactory,
            MessageConsumer acceptThrottle,
            long acceptStreamId)
    {
        this.streamFactory = streamFactory;
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = acceptStreamId;
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

    private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = streamFactory.beginRO.wrap(buffer, index, index + length);
            this.acceptName = begin.source().asString();
            handleBegin(begin);
        }
        else
        {
            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
        }
    }

    private void handleBegin(BeginFW begin)
    {
        long acceptRef = streamFactory.beginRO.sourceRef();
        final RouteFW connectRoute = streamFactory.resolveTarget(acceptRef, acceptName);

        if (connectRoute == null)
        {
            // just reset
            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
        }
        else
        {
            // TODO consider late initialization of this?
            this.connectName = connectRoute.target().asString();
            this.connect = streamFactory.router.supplyTarget(connectName);
            this.connectRef = connectRoute.targetRef();
            this.connectCorrelationId = streamFactory.supplyCorrelationId.getAsLong();
            this.connectStreamId = streamFactory.supplyStreamId.getAsLong();

            this.acceptReply = streamFactory.router.supplyTarget(acceptName);
            this.acceptReplyStreamId = streamFactory.supplyStreamId.getAsLong();
            this.acceptCorrelationId = begin.correlationId();

            final OctetsFW extension = streamFactory.beginRO.extension();
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

            // Should already be canonicalized in http / http2 nuklei
            final String requestURL = getRequestURL(requestHeaders);

            this.requestURLHash = requestURL.hashCode();

            if (requestHeaders.anyMatch(PREFER_RESPONSE_WHEN_MODIFIED))
            {
                storeRequest(requestHeaders);
                handleRequestForWhenModified();
            }
            else if (canBeServedByCache(requestHeaders))
            {
                storeRequest(requestHeaders);
                handleCacheableRequest(requestHeaders, requestURL);
            }
            else
            {
                proxyRequest(requestHeaders);
            }
        }
    }

    private void handleRequestForWhenModified()
    {
        OnModification onModificationRequest;
        this.request = onModificationRequest = new OnModification(
            acceptName,
            acceptReply,
            acceptReplyStreamId,
            acceptCorrelationId,
            streamFactory.streamBufferPool,
            requestSlot,
            requestSize,
            streamFactory.router,
            requestURLHash);
        streamFactory.cache.onUpdate(onModificationRequest);
    }

    private void handleCacheableRequest(
        final ListFW<HttpHeaderFW> requestHeaders,
        final String requestURL)
    {
        Optional<CacheEntry> cachedResponse = streamFactory.cache.getResponseThatSatisfies(
                requestURLHash,
                requestHeaders);

        if (cachedResponse.isPresent())
        {
            CacheableRequest cacheableRequest;
            this.request = cacheableRequest = new CacheableRequest(
                    acceptName,
                    acceptReply,
                    acceptReplyStreamId,
                    acceptCorrelationId,
                    requestURLHash,
                    streamFactory.streamBufferPool,
                    requestSlot,
                    requestSize, streamFactory.router);
            cachedResponse.get().serveClient(cacheableRequest);
        }
        else if(requestHeaders.anyMatch(CacheDirectives.IS_ONLY_IF_CACHED))
        {
            send504();
        }
        else
        {
            this.request = new CacheableRequest(
                    acceptName,
                    acceptReply,
                    acceptReplyStreamId,
                    acceptCorrelationId,
                    requestURLHash,
                    streamFactory.streamBufferPool,
                    requestSlot,
                    requestSize, streamFactory.router);

            sendBeginToConnect(requestHeaders);
            streamFactory.writer.doHttpEnd(connect, connectStreamId);
        }
        this.streamState = this::handleAllFramesByIgnoring;
    }

    private void proxyRequest(
            final ListFW<HttpHeaderFW> requestHeaders)
    {
        this.request = new ProxyRequest(
                acceptName,
                acceptReply,
                acceptReplyStreamId,
                acceptCorrelationId,
                streamFactory.router);

        sendBeginToConnect(requestHeaders);

        this.streamState = this::handleFramesWhenProxying;
    }

    private void sendBeginToConnect(final ListFW<HttpHeaderFW> requestHeaders)
    {
        streamFactory.correlations.put(connectCorrelationId, request);

        streamFactory.writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId,
                builder -> requestHeaders.forEach(
                        h ->  builder.item(item -> item.name(h.name()).value(h.value()))
            )
        );

        streamFactory.router.setThrottle(connectName, connectStreamId, this::handleConnectThrottle);
    }

    private int storeRequest(final ListFW<HttpHeaderFW> headers)
    {
        this.requestSlot = streamFactory.streamBufferPool.acquire(acceptStreamId);
        if (requestSlot == NO_SLOT)
        {
            send503AndReset();
            throw new RuntimeException("Cache out of space, please reconfigure");  // DPW TODO reconsider hard fail??
        }
        this.requestSize = 0;
        MutableDirectBuffer requestCacheBuffer = streamFactory.streamBufferPool.buffer(requestSlot);
        headers.forEach(h ->
        {
            requestCacheBuffer.putBytes(this.requestSize, h.buffer(), h.offset(), h.sizeof());
            this.requestSize += h.sizeof();
        });
        return this.requestSize;
    }

    private void send503AndReset()
    {
        streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
        streamFactory.writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
        e.item(h -> h.representation((byte) 0)
                .name(STATUS)
                .value("503")));
        streamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
    }

    private void send504()
    {
        streamFactory.writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                e.item(h -> h.representation((byte) 0)
                        .name(STATUS)
                        .value("504")));
        streamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
    }

    private void handleAllFramesByIgnoring(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
            default:
        }
    }

    private void handleFramesWhenProxying(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
            final OctetsFW payload = data.payload();
            streamFactory.writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
            break;
        case EndFW.TYPE_ID:
            streamFactory.writer.doHttpEnd(connect, connectStreamId);
            break;
        case AbortFW.TYPE_ID:
            streamFactory.writer.doAbort(connect, connectStreamId);
            break;
        default:
            streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
            break;
        }
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
                final WindowFW window = streamFactory.windowRO.wrap(buffer, index, index + length);
                final int bytes = window.update();
                final int frames = window.frames();
                streamFactory.writer.doWindow(acceptThrottle, acceptStreamId, bytes, frames);
                break;
            case ResetFW.TYPE_ID:
                streamFactory.writer.doReset(acceptThrottle, acceptStreamId);
                break;
            default:
                // TODO, ABORT and RESET
                break;
        }
    }

}