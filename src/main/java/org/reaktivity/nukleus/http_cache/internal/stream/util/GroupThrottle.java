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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;

public class GroupThrottle
{
    private final Long2LongHashMap streamToWaterMark;
    private final long connectReplyStreamId;
    private final MessageConsumer connectReply;
    private final Writer writer;

    private long groupWaterMark = 0;
    private int numParticipants;
    private int padding;

    public GroupThrottle(
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

    private void increment(long stream, long credit, int padding)
    {
        // The padding should be same across all streams for now.
        // TODO may need to keep track of padding across all streams
        this.padding = padding;
        streamToWaterMark.put(stream, streamToWaterMark.get(stream) + credit);
        updateThrottle();
    }

    private void updateThrottle()
    {
        if (numParticipants == streamToWaterMark.size())
        {
            long newLowWaterMark = streamToWaterMark.values().stream().min(Long::compare).orElse(groupWaterMark);
            long diff = newLowWaterMark - groupWaterMark;
            if (diff > 0)
            {
                writer.doWindow(connectReply, connectReplyStreamId, (int)diff, padding);
                groupWaterMark = newLowWaterMark;
            }
        }
    }

    public void processWindow(
        long streamId,
        int credit,
        int padding)
    {
        if (credit > 0)
        {
            increment(streamId, credit, padding);
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

}

