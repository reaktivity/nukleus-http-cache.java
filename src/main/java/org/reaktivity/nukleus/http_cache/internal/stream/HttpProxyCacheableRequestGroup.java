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

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;

import java.util.Map;

import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;

final class HttpProxyCacheableRequestGroup
{
    private final Int2ObjectHashMap<Long2LongHashMap> requestGroups;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer signaler;

    HttpProxyCacheableRequestGroup(
        Writer writer,
        HttpCacheProxyFactory factory)
     {
         this.writer = writer;
         this.factory = factory;
         this.signaler = factory.router.supplyReceiver(0L);
         this.requestGroups = new Int2ObjectHashMap<>();
     }

     boolean queue(
         int requestHash,
         long acceptReplyId,
         long acceptRouteId)
     {
         Long2LongHashMap groupStreams = requestGroups.computeIfAbsent(requestHash,
                                                                       streams -> new Long2LongHashMap(0L));
         boolean queueExists = !groupStreams.isEmpty();
         groupStreams.put(acceptReplyId, acceptRouteId);

         return queueExists;
     }

     void unqueue(
         int requestHash,
         long acceptReplyId)
     {
         Long2LongHashMap groupStreams = requestGroups.get(requestHash);
         assert groupStreams != null;
         groupStreams.remove(acceptReplyId);
         if (groupStreams.isEmpty())
         {
             requestGroups.remove(requestHash);
         }
     }

     void serveNextIfPossible(
         int requestHash,
         long acceptReplyId)
     {
         Long2LongHashMap groupStreams = requestGroups.get(requestHash);
         if (groupStreams != null)
         {
             groupStreams.remove(acceptReplyId);
             if (groupStreams.isEmpty())
             {
                 requestGroups.remove(requestHash);
             }
             else
             {
                Map.Entry<Long, Long> stream = groupStreams.entrySet().iterator().next();
                sendSignalToSubscriber(stream.getKey(),
                                       stream.getValue(),
                                       INITIATE_REQUEST_SIGNAL);
             }
         }
     }

    void signalCacheUpdate(int requestHash)
    {
        this.sendSignalToQueuedInitialRequestSubscribers(requestHash, CACHE_ENTRY_UPDATED_SIGNAL);
    }

    void signalAbort(
        int requestHash)
    {
        this.sendSignalToQueuedInitialRequestSubscribers(requestHash, ABORT_SIGNAL);
    }

    private void sendSignalToQueuedInitialRequestSubscribers(
        int requestHash,
        long signal)
    {

        Long2LongHashMap groupStreams = requestGroups.get(requestHash);
        if (groupStreams != null)
        {
            groupStreams.forEach((acceptReplyId, acceptRouteId) ->
            {
                writer.doSignal(signaler,
                                acceptRouteId,
                                acceptReplyId,
                                factory.supplyTrace.getAsLong(),
                                signal);
            });
        }
    }

    private void sendSignalToSubscriber(
        long acceptReplyId,
        long acceptRouteId,
        long signalId)
    {
         writer.doSignal(signaler,
                         acceptRouteId,
                         acceptReplyId,
                         factory.supplyTrace.getAsLong(),
                         signalId);
    }
}
