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

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

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
    private final LongSupplier supplyTraceId;
    private final Writer writer;

    public ServerStreamFactory(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.router = requireNonNull(router);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.writer = new Writer(supplyTypeId, writeBuffer);
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

            newStream = new ServerAcceptStream(acceptReply, acceptRouteId, acceptInitialId, supplyTraceId)::onStreamMessage;
        }

        return newStream;
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer acceptReply;
        private final long acceptRouteId;
        private final long acceptInitialId;
        private final LongSupplier supplyTrace;

        private MessageConsumer streamState;
        private long acceptReplyId;

        private ServerAcceptStream(
            MessageConsumer acceptReply,
            long acceptRouteId,
            long acceptInitialId,
            LongSupplier supplyTrace)
        {
            this.acceptReply = acceptReply;
            this.acceptRouteId = acceptRouteId;
            this.acceptInitialId = acceptInitialId;
            this.supplyTrace = supplyTrace;
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
                writer.doReset(acceptReply, acceptRouteId, acceptInitialId, 0L, 0L, 0, supplyTrace.getAsLong());
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
                writer.doReset(acceptReply, acceptRouteId, acceptInitialId, 0L, 0L, 0, supplyTrace.getAsLong());
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
            writer.doWindow(acceptReply,
                            acceptRouteId,
                            begin.streamId(),
                            begin.sequence(),
                            begin.acknowledge(),
                            0,
                            this.supplyTrace.getAsLong(),
                            0L,
                            0);
            writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, 0L, 0L, 0, begin.traceId(), hs ->
            {
                hs.item(h -> h.name(":status").value("200"));
                hs.item(h -> h.name("content-type").value("text/event-stream"));
            });
            this.streamState = this::afterBegin;
            router.setThrottle(acceptReplyId, this::onThrottleMessage);
        }

        private void onData(
            final DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final int maximum = data.maximum();
            writer.doReset(acceptReply, acceptRouteId, acceptInitialId, sequence, acknowledge, maximum, supplyTrace.getAsLong());
        }

        private void onEnd(
            final EndFW end)
        {
            // ignore
        }

        private void onAbort(
            final AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final int maximum = abort.maximum();
            final long traceId = abort.traceId();
            writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, sequence, acknowledge, maximum, traceId);
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
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final int maximum = reset.maximum();
            final long traceId = reset.traceId();
            writer.doReset(acceptReply, acceptRouteId, acceptInitialId, sequence, acknowledge, maximum, traceId);
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
