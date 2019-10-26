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
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheProxyEncoder;

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

        private int total;
        private int available;

        private GroupBudget(
            long groupId)
        {
            this.groupId = groupId;
            this.streamBudgetsById = new Long2ObjectHashMap<>();
        }

        public StreamBudget supplyStreamBudget(
            long streamId,
            HttpCacheProxyEncoder encoder)
        {
            return streamBudgetsById.computeIfAbsent(streamId, id -> new StreamBudget(id, encoder));
        }

        @Override
        public String toString()
        {
            return String.format("groupId = %d, available = %d, streamBudgetsById = %s", groupId, available, streamBudgetsById);
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
            final int limit = groupId != 0 ? Math.min(stream.available, available) : stream.available;
            if (limit > 0)
            {
                final int remaining = stream.encoder.encode(limit, traceId);
                final int used = limit - remaining;
                stream.adjust(-used);
            }
        }

        public final class StreamBudget
        {
            private final long streamId;
            private final HttpCacheProxyEncoder encoder;

            private int total;
            private int available;


            private StreamBudget(
                long streamId,
                HttpCacheProxyEncoder encoder)
            {
                this.streamId = streamId;
                this.encoder = encoder;
            }

            public void adjust(
                int delta)
            {
                available += delta;
                assert available >= 0;

                final int oldTotal = total;
                total = Math.max(available, oldTotal);

                if (groupId != 0L && (GroupBudget.this.total == 0 || oldTotal != 0))
                {
                    GroupBudget.this.available += delta;
                    assert GroupBudget.this.available >= 0;

                    GroupBudget.this.total = Math.max(total, GroupBudget.this.total);
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
                return available == total;
            }

            public void close()
            {
                final int inflight = total - available;

                if (groupId != 0L)
                {
                    GroupBudget.this.available += inflight;
                    assert GroupBudget.this.available >= 0;
                }

                streamBudgetsById.remove(streamId);

                if (streamBudgetsById.isEmpty())
                {
                    groupBudgetsById.remove(groupId);
                }
            }

            @Override
            public String toString()
            {
                return String.format("streamId = %d, available = %s", streamId, available);
            }
        }
    }
}
