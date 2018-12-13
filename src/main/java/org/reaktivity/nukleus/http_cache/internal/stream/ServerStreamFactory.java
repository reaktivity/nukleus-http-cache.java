/**
 * Copyright 2016-2018 The Reaktivity Project
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

import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ServerStreamFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final RouteManager router;

    private final LongUnaryOperator supplyReplyId;
    private final Writer writer;

    public ServerStreamFactory(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyReplyId)
    {
        this.router = requireNonNull(router);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.writer = new Writer(writeBuffer);
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

        MessageConsumer newStream = null;

        if (sourceRef != 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer source)
    {
        final long sourceRef = begin.sourceRef();
        final String sourceName = begin.source().asString();
        final long authorization = begin.authorization();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            return sourceRef == route.sourceRef() &&
                    sourceName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(authorization, filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long sourceRouteId = begin.routeId();
            final long sourceId = begin.streamId();

            newStream = new ServerAcceptStream(source, sourceRouteId, sourceId)::onStreamMessage;
        }

        return newStream;
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private final long acceptRouteId;
        private final long acceptStreamId;

        private MessageConsumer streamState;
        private MessageConsumer acceptReply;
        private long acceptReplyStreamId;

        private ServerAcceptStream(
            MessageConsumer acceptThrottle,
            long acceptRouteId,
            long acceptStreamId)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptRouteId = acceptRouteId;
            this.acceptStreamId = acceptStreamId;
            this.streamState = this::beforeBegin;
        }

        private void onStreamMessage(
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
                onBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, acceptRouteId, acceptStreamId, 0L);
            }
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
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            default:
                writer.doReset(acceptThrottle, acceptRouteId, acceptStreamId, 0L);
                break;
            }
        }

        private void onThrottleMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long sourceId = begin.streamId();
            final String sourceName = begin.source().asString();

            this.acceptReply = router.supplyTarget(sourceName);
            this.acceptReplyStreamId =  supplyReplyId.applyAsLong(sourceId);
            final long acceptCorrelationId = begin.correlationId();

            writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyStreamId, acceptCorrelationId, hs ->
            {
                hs.item(h -> h.representation((byte) 0).name(":status").value("200"));
                hs.item(h -> h.representation((byte) 0).name("content-type").value("text/event-stream"));
            });
            this.streamState = this::afterBegin;
            router.setThrottle(sourceName, acceptReplyStreamId, this::onThrottleMessage);
        }

        private void onData(
            final DataFW data)
        {
            writer.doReset(acceptThrottle, acceptRouteId, acceptStreamId, 0L);
        }

        private void onEnd(
            final EndFW end)
        {
            // ignore
        }

        private void onAbort(
            final AbortFW abort)
        {
            final long traceId = abort.trace();
            writer.doAbort(acceptReply, acceptRouteId, acceptReplyStreamId, traceId);
        }

        private void onWindow(
            WindowFW window)
        {
            // NOOP, this means we can send data,
            // but (for now) we don't want to...
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            writer.doReset(acceptThrottle, acceptRouteId, acceptStreamId, traceId);
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
}
