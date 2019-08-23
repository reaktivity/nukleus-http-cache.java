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

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;

import java.util.Map;
import java.util.function.Consumer;

import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;

final class HttpProxyCacheableRequestGroup
{
    private final Long2LongHashMap streamsGroup;
    private final int requestHash;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer signaler;
    private final Consumer<Integer> cleaner;


    HttpProxyCacheableRequestGroup(
        int requestHash,
        Writer writer,
        HttpCacheProxyFactory factory,
        Consumer<Integer> cleaner)
    {
        this.requestHash = requestHash;
        this.writer = writer;
        this.factory = factory;
        this.signaler = factory.router.supplyReceiver(0L);
        this.cleaner = cleaner;
        this.streamsGroup = new Long2LongHashMap(0L);
    }

    boolean queue(
         long acceptReplyId,
         long acceptRouteId)
     {
         boolean queueExists = !streamsGroup.isEmpty();
         streamsGroup.put(acceptReplyId, acceptRouteId);

         return queueExists;
     }

     void unqueue(
         long acceptReplyId)
     {
         streamsGroup.remove(acceptReplyId);
         if (streamsGroup.isEmpty())
         {
             cleaner.accept(requestHash);
         }
     }

     void removeRequestAndResumeNextRequest(
         long acceptReplyId)
     {
         streamsGroup.remove(acceptReplyId);
         serveNextRequestIfPossible();
     }

    void onCacheableResponseUpdated()
    {
        this.sendSignalToQueuedInitialRequestSubscribers(CACHE_ENTRY_UPDATED_SIGNAL);
    }

    void onCacheableResponseAborted()
    {
        this.sendSignalToQueuedInitialRequestSubscribers(ABORT_SIGNAL);
    }

    private void sendSignalToQueuedInitialRequestSubscribers(
        long signal)
    {
            streamsGroup.forEach((acceptReplyId, acceptRouteId) ->
            {
                writer.doSignal(signaler,
                                acceptRouteId,
                                acceptReplyId,
                                factory.supplyTrace.getAsLong(),
                                signal);
            });
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

    private void serveNextRequestIfPossible()
    {
        if (streamsGroup.isEmpty())
        {
            cleaner.accept(requestHash);
        }
        else
        {
            Map.Entry<Long, Long> stream = streamsGroup.entrySet().iterator().next();
            sendSignalToSubscriber(stream.getKey(),
                                   stream.getValue(),
                                   INITIATE_REQUEST_SIGNAL);
        }
    }
}
