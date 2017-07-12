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
package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;

public class GroupThrottle
{
    private final Long2LongHashMap streamToWaterMark;
    private final long connectReplyStreamId;
    private final MessageConsumer connectReply;
    private final Writer writer;

    private long groupWaterMark = 0;
    private int numParticipants;

    GroupThrottle(
        int numParticipants,
        Writer writer,
        MessageConsumer connectReply,
        long connectReplyStreamId)
    {
        this.numParticipants = numParticipants;
        this.writer = writer;
        this.connectReplyStreamId = connectReplyStreamId;
        this.connectReply = connectReply;
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
                // TODO, track frames?
                writer.doWindow(connectReply, connectReplyStreamId, (int)diff, (int)diff);
                groupWaterMark = newLowWaterMark;
            }
        }
    }

    public void processWindow(
        long streamId,
        int update)
    {
        if (update > 0)
        {
            increment(streamId, update);
        }
    }

    public void processReset(long streamId)
    {
        streamToWaterMark.remove(streamId);
        this.numParticipants--;
        if (this.numParticipants == 0)
        {
            writer.doReset(connectReply, connectReplyStreamId);
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

