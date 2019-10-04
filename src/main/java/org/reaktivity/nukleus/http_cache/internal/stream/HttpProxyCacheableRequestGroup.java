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
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_NOT_MODIFIED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_IN_FLIGHT_ABORT_SIGNAL;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;

final class HttpProxyCacheableRequestGroup
{
    private final HashMap<String, Long2LongHashMap> requestsQueue;
    private final int requestHash;
    private String etag;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final Consumer<Integer> cleaner;
    private String recentAuthorizationToken;

    public boolean isInFlightRequestAborted()
    {
        return isInFlightRequestAborted;
    }

    private boolean isInFlightRequestAborted;

    HttpProxyCacheableRequestGroup(
        int requestHash,
        String etag,
        Writer writer,
        HttpCacheProxyFactory factory,
        Consumer<Integer> cleaner)
    {
        this.requestHash = requestHash;
        this.etag = etag;
        this.writer = writer;
        this.factory = factory;
        this.cleaner = cleaner;
        this.requestsQueue = new HashMap<>();
    }

    boolean isItLiveRequest(
        String etag)
    {
        return (this.etag == null) || this.etag.equals(etag);
    }

    String getRecentAuthorizationToken()
    {
        return recentAuthorizationToken;
    }

    void setRecentAuthorizationToken(
        String recentAuthorizationToken)
    {
        this.recentAuthorizationToken = recentAuthorizationToken;
    }

    int getNumberOfRequests()
    {
        AtomicInteger totalRequests = new AtomicInteger();
        requestsQueue.forEach((key, routeIdsByReplyId) ->
        {
            totalRequests.addAndGet(routeIdsByReplyId.size());
        });
        return totalRequests.get();
    }

    boolean enqueue(
        String etag,
        long acceptRouteId,
        long acceptReplyId)
    {
        Long2LongHashMap routeIdsByReplyId = requestsQueue.computeIfAbsent(etag, this::createQueue);
        routeIdsByReplyId.put(acceptReplyId, acceptRouteId);
        if (this.etag != null &&
            !this.etag.equals(etag))
        {
            isInFlightRequestAborted = true;
            abortInFlightRequest(this.etag);
            this.etag = etag;
            return false;
        }
        else
        {
            boolean isEnqueued = requestsQueue.size() > 1 || routeIdsByReplyId.size() != 1;
            if (!isEnqueued)
            {
                this.etag = etag;
            }
            return isEnqueued;
        }
    }

    void dequeue(
        String etag,
        long acceptReplyId)
    {
        Long2LongHashMap routeIdsByReplyId = requestsQueue.get(etag);
        if (routeIdsByReplyId != null)
        {
            routeIdsByReplyId.remove(acceptReplyId);
            if (routeIdsByReplyId.isEmpty())
            {
                requestsQueue.remove(etag);
            }
        }

        if (requestsQueue.isEmpty())
        {
            cleaner.accept(requestHash);
        }
    }

    void onNonCacheableResponse(
        String etag,
        long acceptReplyId)
    {
        Long2LongHashMap routeIdsByReplyId = requestsQueue.get(etag);
        if (routeIdsByReplyId != null)
        {
            routeIdsByReplyId.remove(acceptReplyId);
        }
        serveNextRequestIfPossible(etag);
    }

    void onCacheableResponseUpdated(
        String etag)
    {
        requestsQueue.forEach((key, routeIdsByReplyId) ->
        {
            if (key != null && key.equals(etag) ||
                (key == null && etag == null) && requestsQueue.size() > 1)
            {
                routeIdsByReplyId.forEach(this::doSignalCacheEntryNotModified);
            }
            else
            {
                routeIdsByReplyId.forEach(this::doSignalCacheEntryUpdated);
            }
        });
    }

    void onCacheableResponseAborted()
    {
        requestsQueue.forEach((key, routeIdsByReplyId) ->
        {
            routeIdsByReplyId.forEach(this::doSignalCacheEntryAborted);
        });
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

    private void doSignalCacheEntryNotModified(
        long acceptReplyId,
        long acceptRouteId)
    {
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        CACHE_ENTRY_NOT_MODIFIED_SIGNAL);

    }

    private void serveNextRequestIfPossible(
        String etag)
    {
        cleanup(etag);

        if (!requestsQueue.isEmpty())
        {

            Map.Entry<String, Long2LongHashMap> routeIdsByReplyId = requestsQueue.entrySet().iterator().next();
            Map.Entry<Long, Long> stream = routeIdsByReplyId.getValue().entrySet().iterator().next();
            writer.doSignal(stream.getKey(),
                            stream.getValue(),
                            factory.supplyTrace.getAsLong(),
                            INITIATE_REQUEST_SIGNAL);
        }
        else
        {
            cleaner.accept(requestHash);
        }
    }

    private Long2LongHashMap createQueue(
        String etag)
    {
        Long2LongHashMap queue =  requestsQueue.get(etag);
        return Objects.requireNonNullElseGet(queue, () -> new Long2LongHashMap(-1));
    }

    private void abortInFlightRequest(
        String etag)
    {
        Long2LongHashMap routeIdsByReplyId = requestsQueue.get(etag);
        routeIdsByReplyId.forEach(this::doSignalInFlightRequestAborted);
    }

    private void doSignalInFlightRequestAborted(
        long acceptReplyId,
        long acceptRouteId)
    {
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        REQUEST_IN_FLIGHT_ABORT_SIGNAL);
    }

    private void cleanup(
        String etag)
    {
        Long2LongHashMap routeIdsByReplyId = requestsQueue.get(etag);
        if (routeIdsByReplyId != null &&
            routeIdsByReplyId.isEmpty())
        {
            requestsQueue.remove(etag);
            if (requestsQueue.isEmpty())
            {
                cleaner.accept(requestHash);
            }
        }

        requestsQueue.forEach((key, otherRouteIdsByReplyId) ->
        {
            if (otherRouteIdsByReplyId.isEmpty())
            {
                requestsQueue.remove(key);
            }
        });
    }
}
