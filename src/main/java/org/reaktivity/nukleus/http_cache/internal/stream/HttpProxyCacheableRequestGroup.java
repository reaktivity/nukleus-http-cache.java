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

import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_ABORTED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;

import java.util.Map;
import java.util.function.Consumer;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;

final class HttpProxyCacheableRequestGroup
{
    private final Long2LongHashMap routeIdsByReplyId;
    private final int requestHash;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final Consumer<Integer> cleaner;
    private String recentAuthorizationToken;

    HttpProxyCacheableRequestGroup(
        int requestHash,
        Writer writer,
        HttpCacheProxyFactory factory,
        Consumer<Integer> cleaner)
    {
        this.requestHash = requestHash;
        this.writer = writer;
        this.factory = factory;
        this.cleaner = cleaner;
        this.routeIdsByReplyId = new Long2LongHashMap(-1L);
    }

    String getRecentAuthorizationToken()
    {
        return recentAuthorizationToken;
    }

    void setRecentAuthorizationToken(String recentAuthorizationToken)
    {
        this.recentAuthorizationToken = recentAuthorizationToken;
    }

    int getNumberOfRequests()
    {
        return routeIdsByReplyId.size();
    }

    boolean queue(
        long acceptRouteId,
        long acceptReplyId)
    {
        routeIdsByReplyId.put(acceptReplyId, acceptRouteId);
        return routeIdsByReplyId.size() > 1;
    }

    void unqueue(
        long acceptReplyId)
    {
        routeIdsByReplyId.remove(acceptReplyId);
        if (routeIdsByReplyId.isEmpty())
        {
            cleaner.accept(requestHash);
        }
    }

    void onNonCacheableResponse(
        long acceptReplyId)
    {
        routeIdsByReplyId.remove(acceptReplyId);
        serveNextRequestIfPossible();
    }

    void onCacheableResponseUpdated()
    {
        routeIdsByReplyId.forEach(this::doSignalCacheEntryUpdated);
    }

    void onCacheableResponseAborted()
    {
        this.routeIdsByReplyId.forEach(this::doSignalCacheEntryAborted);
    }

    private void doSignalCacheEntryAborted(
        long acceptReplyId,
        long acceptRouteId)
    {
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        CACHE_ENTRY_ABORTED_SIGNAL);
    }

    private void doSignalCacheEntryUpdated(
        long acceptReplyId,
        long acceptRouteId)
    {
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        CACHE_ENTRY_UPDATED_SIGNAL);

    }

    private void sendSignalToSubscriber(
        long acceptReplyId,
        long acceptRouteId,
        long signalId)
    {
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        signalId);
    }

    private void serveNextRequestIfPossible()
    {
        if (routeIdsByReplyId.isEmpty())
        {
            cleaner.accept(requestHash);
        }
        else
        {
            Map.Entry<Long, Long> stream = routeIdsByReplyId.entrySet().iterator().next();
            sendSignalToSubscriber(stream.getKey(),
                                   stream.getValue(),
                                   INITIATE_REQUEST_SIGNAL);
        }
    }
}
