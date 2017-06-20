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

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http_cache.internal.routable.Source;
import org.reaktivity.nukleus.http_cache.internal.routable.Target;
import org.reaktivity.nukleus.http_cache.internal.routable.stream.ProxyAcceptStreamFactory.SourceInputStream;
import org.reaktivity.nukleus.http_cache.internal.router.Correlation;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class ProxyConnectReplyStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final Function<String, Target> supplyTarget;
    private final LongSupplier supplyStreamId;
    private final LongFunction<Correlation> correlateEstablished;

    private Int2ObjectHashMap<List<SourceInputStream>> awaitingRequestMatches;

    private Int2ObjectHashMap<SourceInputStream> urlToPendingStream;

    public ProxyConnectReplyStreamFactory(
        Source source,
        Function<String, Target> supplyTarget,
        LongSupplier supplyStreamId,
        LongFunction<Correlation> correlateEstablished,
        Int2ObjectHashMap<SourceInputStream> urlToPendingStream,
        Int2ObjectHashMap<List<SourceInputStream>> awaitingRequestMatches)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.supplyStreamId = supplyStreamId;
        this.correlateEstablished = correlateEstablished;
        this.awaitingRequestMatches = awaitingRequestMatches;
        this.urlToPendingStream = urlToPendingStream;
    }

    public MessageHandler newStream()
    {
        return new TargetOutputEstablishedStream()::handleStream;
    }

    private final class TargetOutputEstablishedStream
    {
        private MessageHandler streamState;

        private long sourceId;

        private Target target;
        private long targetId;
        private List<SourceInputStream> forwardResponsesTo;

        private TargetOutputEstablishedStream()
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
            MutableDirectBuffer buffer,
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
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if(forwardResponsesTo != null)
            {
                this.forwardResponsesTo.stream().forEach(
                    h -> h.replyMessageHandler().onMessage(msgTypeId, buffer, index, length)
                );
            }
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
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

        private void afterRejectOrReset(
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

            this.streamState = this::afterRejectOrReset;
        }

        private void processBegin(
            final MutableDirectBuffer buffer,
            final int index,
            final int length)
        {
            beginRO.wrap(buffer, index, index + length);

            final long newSourceId = beginRO.streamId();
            final long sourceRef = beginRO.sourceRef();
            final long targetCorrelationId = beginRO.correlationId();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);
            int requestURLHash = correlation.requestURLHash();

            this.forwardResponsesTo = awaitingRequestMatches.remove(requestURLHash);
            if(requestURLHash == -1 || forwardResponsesTo == null)
            {
                forwardResponsesTo = emptyList();
            }

            if (sourceRef == 0L && correlation != null)
            {
                final Target newTarget = supplyTarget.apply(correlation.source());
                final long newTargetId = supplyStreamId.getAsLong();
                final long sourceCorrelationId = correlation.id();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);

                newTarget.doHttpBegin(newTargetId, 0L, sourceCorrelationId, e ->
                {
                    httpBeginEx.headers().forEach(h ->
                    {
                       e.item(h2 -> h2.representation((byte)0).name(h.name().asString()).value(h.value().asString()));
                    });
                });
                GroupThrottle replyThrottle = new GroupThrottle(this.forwardResponsesTo.size() +1, source, newSourceId);

                this.forwardResponsesTo.stream().forEach(sourceInputStream ->
                    {
                        sourceInputStream.setReplyThrottle(replyThrottle);
                        sourceInputStream.replyMessageHandler().onMessage(BeginFW.TYPE_ID, buffer, index, length);
                    }
                );

                SourceInputStream pendingStream = urlToPendingStream.remove(requestURLHash);
                if(pendingStream != null)
                {
                    pendingStream.clean();
                }

                newTarget.addThrottle(newTargetId, replyThrottle::handleThrottle);

                this.sourceId = newSourceId;
                this.target = newTarget;
                this.targetId = newTargetId;

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);
            target.doHttpData(targetId, dataRO.payload(), dataRO.extension());
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);

            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);
            source.removeStream(sourceId);
        }
    }

    public class GroupThrottle
    {
        long groupWaterMark = 0;
        private final Long2LongHashMap streamToWaterMark;
        private final long replyStreamId;
        private int numParticipants;

        private Source source;

        GroupThrottle(
            int numParticipants,
            Source source,
            long replyStreamId)
        {
            this.numParticipants = numParticipants;
            this.replyStreamId = replyStreamId;
            this.source = source;
            streamToWaterMark = new Long2LongHashMap(0);
        }

        private void increment(long stream, long increment)
        {
            streamToWaterMark.put(stream, streamToWaterMark.get(stream) + increment);
            updateThrottle();
        }

        private void updateThrottle()
        {
            if (numParticipants == streamToWaterMark.size())
            {
                long newLowWaterMark = streamToWaterMark.values().stream().min(Long::compare).get();
                long diff = newLowWaterMark - groupWaterMark;
                if (diff > 0)
                {
                    source.doWindow(replyStreamId, (int) diff);
                    groupWaterMark = newLowWaterMark;
                }
            }
        }

        public void handleThrottle(
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
            int update = windowRO.update();
            if(update > 0)
            {
                increment(windowRO.streamId(), update);
            }
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);
            streamToWaterMark.remove(resetRO.streamId());
            this.numParticipants--;
            if(this.numParticipants == 0)
            {
                source.doReset(replyStreamId);
            }
            else
            {
                updateThrottle();
            }
        }

        public void optOut()
        {
            this.numParticipants--;
        }
    }
}
