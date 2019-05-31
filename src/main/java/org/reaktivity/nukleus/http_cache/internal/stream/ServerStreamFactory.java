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

import java.util.function.LongSupplier;
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
    private final LongSupplier supplyTrace;
    private final Writer writer;

    public ServerStreamFactory(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTrace)
    {
        this.supplyTrace = requireNonNull(supplyTrace);
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
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
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

            newStream = new ServerAcceptStream(acceptReply, acceptRouteId, acceptInitialId)::onStreamMessage;
        }

        return newStream;
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer acceptReply;
        private final long acceptRouteId;
        private final long acceptInitialId;

        private MessageConsumer streamState;
        private long acceptReplyId;

        private ServerAcceptStream(
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptInitialId)
        {
            this.acceptReply = acceptReply;
            this.acceptRouteId = acceptRouteId;
            this.acceptInitialId = acceptInitialId;
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
                writer.doReset(acceptReply, acceptRouteId, acceptInitialId, supplyTrace.getAsLong());
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
                writer.doReset(acceptReply, acceptRouteId, acceptInitialId, supplyTrace.getAsLong());
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
            final long initialId = begin.streamId();

            this.acceptReplyId =  supplyReplyId.applyAsLong(initialId);

            writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, hs ->
            {
                hs.item(h -> h.representation((byte) 0).name(":status").value("200"));
                hs.item(h -> h.representation((byte) 0).name("content-type").value("text/event-stream"));
            }, begin.trace());
            this.streamState = this::afterBegin;
            router.setThrottle(acceptReplyId, this::onThrottleMessage);
        }

        private void onData(
            final DataFW data)
        {
            writer.doReset(acceptReply, acceptRouteId, acceptInitialId, supplyTrace.getAsLong());
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
            writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, traceId);
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
            writer.doReset(acceptReply, acceptRouteId, acceptInitialId, traceId);
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
