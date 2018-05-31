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

import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class BudgetManager
{

    private final Long2ObjectHashMap<GroupBudget> groups;             // group id -> GroupBudget
    private final Random random;

    public enum StreamKind
    {
        CACHE,
        PROXY
    }

    private static class StreamBudget
    {
        final long streamId;
        final StreamKind streamKind;
        int unackedBudget;
        int index;
        IntUnaryOperator budgetAvailable;

        StreamBudget(long streamId, StreamKind kind, IntUnaryOperator budgetAvailable, int index)
        {
            this.streamId = streamId;
            this.streamKind = requireNonNull(kind);
            this.budgetAvailable = budgetAvailable;
            setIndex(index);
        }

        void setIndex(int index)
        {
            this.index = index;
        }

        @Override
        public String toString()
        {
            return String.format("(id=%d kind=%s budget=%d)", streamId, streamKind, unackedBudget);
        }
    }

    private class GroupBudget
    {
        final long groupId;
        final int initialBudget;
        final Long2ObjectHashMap<StreamBudget> streamMap;          // stream id -> BudgetEnty
        final List<StreamBudget> streamList;
        int budget;

        GroupBudget(long groupId, int initialBudget)
        {
            this.groupId = groupId;
            this.initialBudget = initialBudget;
            this.streamMap = new Long2ObjectHashMap<>();
            this.streamList = new ArrayList<>();
        }

        void add(long streamId, StreamBudget streamBudget)
        {
            streamMap.put(streamId, streamBudget);
            streamList.add(streamBudget);
        }

        StreamBudget get(long streamId)
        {
            return streamMap.get(streamId);
        }

        boolean isEmpty()
        {
            return streamMap.isEmpty();
        }

        int size()
        {
            return streamList.size();
        }

        StreamBudget remove(long streamId)
        {
            StreamBudget streamBudget = streamMap.remove(streamId);
            if (streamBudget != null)
            {
                // replace the removed one with the last streamBudget
                StreamBudget last = streamList.get(streamList.size() - 1);
                last.setIndex(streamBudget.index);
                streamList.set(streamBudget.index, last);
                streamList.remove(streamList.size() - 1);
            }
            assert streamMap.size() == streamList.size();
            return streamBudget;
        }

        private void moreBudget(int credit)
        {
            budget += credit;
            assert budget <= initialBudget;

            int start, index;
            start = index = random.nextInt(streamList.size());
            do
            {
                StreamBudget stream = streamList.get(index);
System.out.printf("Giving budget=%d to (groupId=%d, index=%d stream=%s)\n", budget, groupId, index, stream);
                stream.unackedBudget += budget;
                budget = stream.budgetAvailable.applyAsInt(budget);
                stream.unackedBudget -= budget;
                index = (index + 1) % streamList.size();
            }
            while(budget > 0 && index != start);
        }

        @Override
        public String toString()
        {
            long proxyStreams = streamList.stream().filter(s -> s.streamKind == StreamKind.PROXY).count();
            long cacheStreams = streamList.stream().filter(s -> s.streamKind == StreamKind.CACHE).count();
            List<StreamBudget> activeStreams = streamList.stream()
                                                         .filter(s -> s.unackedBudget > 0)
                                                         .collect(Collectors.toList());

            return String.format("(groupId=%d budget=%d proxyStreams=%d cacheStreams=%d activeStreams=%s)",
                    groupId, budget, proxyStreams, cacheStreams, activeStreams);
        }
    }

    BudgetManager()
    {
        groups = new Long2ObjectHashMap<>();
        random = new Random();
    }

    public void done(StreamKind streamKind, long groupId, long streamId)
    {
        if (groupId != 0)
        {
            GroupBudget groupBudget = groups.get(groupId);
            if (groupBudget != null)
            {
                StreamBudget streamBudget = groupBudget.remove(streamId);
                if (groupBudget.isEmpty())
                {
                    groups.remove(groupId);
                }
                else if (streamBudget != null && streamBudget.unackedBudget > 0)
                {
                    groupBudget.moreBudget(streamBudget.unackedBudget);
                }
            }
        }
        System.out.printf("DONE (kind=%s groupId=%d streamId=%d groups=%s)\n", streamKind, groupId, streamId, groups);
    }

    public void window(StreamKind streamKind, long groupId, long streamId, int credit, IntUnaryOperator budgetAvailable)
    {
        if (groupId == 0)
        {
            budgetAvailable.applyAsInt(credit);
        }
        else
        {
            boolean gotBudget = false;
            GroupBudget groupBudget = groups.get(groupId);
            if (groupBudget == null)
            {
                groupBudget = new GroupBudget(groupId, credit);
                groups.put(groupId, groupBudget);
                gotBudget = true;
            }
            StreamBudget streamBudget = groupBudget.get(streamId);
            if (streamBudget == null)
            {
                // Ignore initial window of a stream (except the very first stream)
                int index = groupBudget.size();
                streamBudget = new StreamBudget(streamId, streamKind, budgetAvailable, index);
                groupBudget.add(streamId, streamBudget);
            }
            else
            {
                streamBudget.unackedBudget -= credit;
                assert streamBudget.unackedBudget >= 0;
                gotBudget = true;
            }
System.out.printf("WINDOW (kind=%s groupId=%d streamId=%d credit=%d groupBudget=%s)\n", streamKind, groupId, streamId, credit,
groupBudget);

            if (gotBudget)
            {
                groupBudget.moreBudget(credit);
            }
        }
    }
}
