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

import org.agrona.collections.Long2ObjectHashMap;

public final class BudgetManager
{
    private final Long2ObjectHashMap<GroupBudget> groupBudgetsById;

    public BudgetManager()
    {
        this.groupBudgetsById = new Long2ObjectHashMap<>();
    }

    public GroupBudget supplyGroupBudget(
        long groupId)
    {
        return groupBudgetsById.computeIfAbsent(groupId, GroupBudget::new);
    }

    public final class GroupBudget
    {
        private final long groupId;
        private final Long2ObjectHashMap<StreamBudget> streamBudgetsById;

        private int groupTotal;
        private int groupAvailable;

        private GroupBudget(
            long groupId)
        {
            this.groupId = groupId;
            this.streamBudgetsById = new Long2ObjectHashMap<>();
        }

        public StreamBudget supplyStreamBudget(
            long streamId,
            BudgetEncoder encoder)
        {
            return streamBudgetsById.computeIfAbsent(streamId, id -> new StreamBudget(id, encoder));
        }

        @Override
        public String toString()
        {
            return String.format("groupId = %d, groupAvailable = %d, streamBudgetsById = %s",
                    groupId, groupAvailable, streamBudgetsById);
        }

        private void flush(
            long traceId)
        {
            streamBudgetsById.values().forEach(s -> flushStream(s, traceId));
        }

        private void flushStream(
            StreamBudget stream,
            long traceId)
        {
            // TODO fairness
            final int limit = groupId != 0 ? Math.min(stream.streamAvailable, groupAvailable) : stream.streamAvailable;
            if (limit > 0)
            {
                final int remaining = stream.encoder.encode(limit, traceId);
                final int used = limit - remaining;

                assert used >= 0;

                if (used != 0)
                {
                    stream.adjust(-used);
                }
            }
        }

        public final class StreamBudget
        {
            private final long streamId;
            private final BudgetEncoder encoder;

            private int streamTotal;
            private int streamAvailable;


            private StreamBudget(
                long streamId,
                BudgetEncoder encoder)
            {
                this.streamId = streamId;
                this.encoder = encoder;
            }

            public void adjust(
                int delta)
            {
                final boolean groupInit = groupTotal == 0;
                final boolean groupUpdate = streamTotal != 0;

                final int newStreamAvailable = streamAvailable + delta;
                assert newStreamAvailable >= 0;

                final int newStreamTotal = newStreamAvailable > streamTotal ? newStreamAvailable : streamTotal;
                final int newStreamTotalDiff = newStreamTotal - streamTotal;
                assert newStreamTotalDiff >= 0;

                streamAvailable = newStreamAvailable;
                streamTotal = newStreamTotal;

                assert streamAvailable <= streamTotal : String.format("%d <= %d", streamAvailable, streamTotal);

                if (groupId != 0L && (groupInit || groupUpdate))
                {
                    final int groupDelta = groupInit ? delta : delta - newStreamTotalDiff;

                    groupAvailable += groupDelta;
                    groupTotal = Math.max(streamTotal, groupTotal);

                    assert groupAvailable >= 0 : String.format("%d >= 0", groupAvailable);
                    assert groupAvailable <= groupTotal : String.format("%d <= %d", groupAvailable, groupTotal);
                }
            }

            public void flushGroup(
                long traceId)
            {
                if (groupId == 0L)
                {
                    if (streamBudgetsById.containsValue(this))
                    {
                        flushStream(this, traceId);
                    }
                }
                else
                {
                    flush(traceId);
                }
            }

            public boolean closeable()
            {
                return streamAvailable == streamTotal;
            }

            public void close()
            {
                final int inflight = streamTotal - streamAvailable;

                streamAvailable += inflight;

                assert streamAvailable <= streamTotal : String.format("%d <= %d", streamAvailable, streamTotal);

                if (groupId != 0L)
                {
                    groupAvailable += inflight;

                    assert groupAvailable >= 0 : String.format("%d >= 0", groupAvailable);
                    assert groupAvailable <= groupTotal : String.format("%d <= %d", groupAvailable, groupTotal);
                }

                final StreamBudget streamBudget = streamBudgetsById.remove(streamId);

                assert streamBudget == this;

                if (streamBudgetsById.isEmpty())
                {
                    final GroupBudget groupBudget = groupBudgetsById.remove(groupId);

                    assert groupBudget == GroupBudget.this;
                }
            }

            @Override
            public String toString()
            {
                return String.format("streamId = %d, streamAvailable = %s", streamId, streamAvailable);
            }
        }
    }

    @FunctionalInterface
    public interface BudgetEncoder
    {
        int encode(int credit, long traceId);
    }
}
