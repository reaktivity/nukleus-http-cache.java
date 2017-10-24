package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class ProxyConnectReplyStream
{
    private final ProxyStreamFactory streamFactory;

    private MessageConsumer streamState;

    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyStreamId;

    private Request streamCorrelation;

    ProxyConnectReplyStream(
            ProxyStreamFactory proxyStreamFactory,
            MessageConsumer connectReplyThrottle,
            long connectReplyId)
    {
        this.streamFactory = proxyStreamFactory;
        this.connectReplyThrottle = connectReplyThrottle;
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
            this.streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    private void handleBegin(
            BeginFW begin)
    {
        final long connectReplyRef = begin.sourceRef();
        final long connectCorrelationId = begin.correlationId();

        this.streamCorrelation = connectReplyRef == 0L ?
            this.streamFactory.correlations.remove(connectCorrelationId) : null;

        if (streamCorrelation != null)
        {
            final OctetsFW extension = streamFactory.beginRO.extension();
            final HttpBeginExFW httpBeginFW = extension.get(streamFactory.httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();

            switch(streamCorrelation.getType())
            {
                case PROXY:
                    doProxyBegin(responseHeaders);
                    break;
                case CACHEABLE:
                    handleCacheableResponse(responseHeaders);
                    break;
                case ON_MODIFIED:
                default:
                    throw new RuntimeException("Not implemented");
            }
        }
        else
        {
            this.streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    ///////////// CACHEABLE REQUEST
    private void handleCacheableResponse(ListFW<HttpHeaderFW> responseHeaders)
    {
        CacheableRequest request = (CacheableRequest) streamCorrelation;
        request.cache(responseHeaders);
        doProxyBegin(responseHeaders);
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
                request.cache(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = streamFactory.endRO.wrap(buffer, index, index + length);
                request.cache(end, streamFactory.cache);
                break;
            case AbortFW.TYPE_ID:
            default:
                request.abort();
                break;
        }
        this.handleFramesWhenProxying(msgTypeId, buffer, index, length);
    }

    ///////////// PROXY
    private void doProxyBegin(ListFW<HttpHeaderFW> responseHeaders)
    {
        final String acceptName = streamCorrelation.acceptName();
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();
        final long acceptReplyRef = streamCorrelation.acceptRef();
        final long correlationId = streamCorrelation.acceptCorrelationId();

        streamFactory.writer.doHttpBegin(
                acceptReply,
                acceptReplyStreamId,
                acceptReplyRef,
                correlationId,
                builder -> responseHeaders.forEach(
                        h -> builder.item(item -> item.name(h.name()).value(h.value()))
            ));
        streamFactory.router.setThrottle(acceptName, acceptReplyStreamId, this::handleProxyThrottle);

        this.streamState = this::handleFramesWhenProxying;
    }

    private void handleFramesWhenProxying(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        final MessageConsumer acceptReply = streamCorrelation.acceptReply();
        final long acceptReplyStreamId = streamCorrelation.acceptReplyStreamId();
        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                final DataFW data = streamFactory.dataRO.wrap(buffer, index, index + length);
                final OctetsFW payload = data.payload();
                streamFactory.writer.doHttpData(
                        acceptReply,
                        acceptReplyStreamId,
                        payload.buffer(),
                        payload.offset(),
                        payload.sizeof());
                break;
            case EndFW.TYPE_ID:
                streamFactory.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                streamCorrelation.complete();
                break;
            case AbortFW.TYPE_ID:
                streamFactory.writer.doAbort(acceptReply, acceptReplyStreamId);
                break;
            default:
                streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                break;
            }
    }

    private void handleProxyThrottle(
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
                streamFactory.writer.doWindow(connectReplyThrottle, connectReplyStreamId, bytes, frames);
                break;
            case ResetFW.TYPE_ID:
                streamFactory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
                streamCorrelation.abort();
                break;
            default:
                // TODO,  ABORT and RESET
                break;
            }
    }

}