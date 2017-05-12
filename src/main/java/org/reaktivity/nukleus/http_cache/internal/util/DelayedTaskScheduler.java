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
package org.reaktivity.nukleus.http_cache.internal.util;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.agrona.collections.Long2ObjectHashMap;

public class DelayedTaskScheduler
{
    private final Long2ObjectHashMap<Runnable> taskLookup;
    private final SortedSet<Long> scheduledTimes;

    public DelayedTaskScheduler()
    {
        this.taskLookup = new Long2ObjectHashMap<>();
        this.scheduledTimes = new TreeSet<>();
    }

    public void schedule(Long time, Runnable task)
    {
        if(this.scheduledTimes.add(time))
        {
            this.taskLookup.put(time, task);
        }
        else
        {
            this.taskLookup.merge(time, task, (t1, t2) ->
            {
                return () ->
                {
                    t1.run();
                    t2.run();
                };
            });
        }
    }

    public void process()
    {
        long c = System.currentTimeMillis();
        Iterator<Long> iter = scheduledTimes.iterator();
        while(iter.hasNext())
        {
            Long s = iter.next();
            if(s > c)
            {
                break;
            }
            scheduledTimes.remove(s);
            taskLookup.remove(s).run();
        }
    }
}
