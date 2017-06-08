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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http_cache.internal.routable.Route;
import org.reaktivity.nukleus.http_cache.internal.routable.Source;
import org.reaktivity.nukleus.http_cache.internal.routable.Target;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class ServerAcceptStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final Function<String, Target> supplyTargetRoute;

    public ServerAcceptStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        Function<String, Target> supplyTargetRoute)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.supplyTargetRoute = supplyTargetRoute;
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;

        private long sourceId;

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
                long streamId = dataRO.wrap(buffer, index, length).streamId();
                source.doReset(streamId);
                break;
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

        private void sendHttpResponseBegin(
            DirectBuffer buffer,
            int index,
            int length,
            long sourceRef,
            long correlation,
            long sourceId,
            String status)
        {
            final Optional<Route> optional = resolveReplyTo(sourceRef);

            if (optional.isPresent())
            {
                final Route route = optional.get();
                final Target replyTo = supplyTargetRoute.apply(route.targetName());
                final long targetRef = route.targetRef();
                final long newTargetId = supplyTargetId.getAsLong();

                replyTo.doHttpBegin(newTargetId, targetRef, correlation,
                        hs ->
                        {
                            hs.item(h -> h.name(":status").value(status));
                            hs.item(h -> h.name("content-type").value("text/event-stream"));
                        });
                this.streamState = this::afterReplyOrReset;
                replyTo.addThrottle(sourceId, this::handleThrottle);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
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
            source.doWindow(newSourceId, 0);

            sendHttpResponseBegin(buffer, index, length, sourceRef, correlationId, newSourceId, "200");

            this.sourceId = newSourceId;
            this.streamState = this::afterBeginOrData;
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();
            source.removeStream(streamId);
        }

        private Optional<Route> resolveReplyTo(
            long sourceRef)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = Route.sourceMatches(source.routableName());

            return routes.stream().filter(predicate).findFirst();
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
    }
}
