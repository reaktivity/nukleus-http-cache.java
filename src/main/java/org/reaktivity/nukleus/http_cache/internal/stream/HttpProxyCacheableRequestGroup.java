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
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.GROUP_REQUEST_RESET_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;

final class HttpProxyCacheableRequestGroup
{
    private final HashMap<String, Long2LongHashMap> requestsQueue;
    private final LongArrayList requestsWithAnswer;
    private final int requestHash;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final Consumer<Integer> cleaner;
    private String etag;
    private long acceptRouteId;
    private long acceptReplyId;
    private String recentAuthorizationToken;
    private long routeId;
    private long replyId;
    private MessageConsumer connect;

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
        this.requestsQueue = new HashMap<>();
        this.requestsWithAnswer = new LongArrayList();
    }

    String getRecentAuthorizationToken()
    {
        return recentAuthorizationToken;
    }

    int getRequestHash()
    {
        return requestHash;
    }

    String getEtag()
    {
        return etag;
    }

    void setRecentAuthorizationToken(
        String recentAuthorizationToken)
    {
        this.recentAuthorizationToken = recentAuthorizationToken;
    }

    int getNumberOfRequests()
    {
        MutableInteger totalRequests = new MutableInteger();
        requestsQueue.forEach((key, routeIdsByReplyId) -> totalRequests.value += routeIdsByReplyId.size());
        return totalRequests.value;
    }

    void enqueue(
        String etag,
        long acceptRouteId,
        long acceptReplyId)
    {
        final boolean requestQueueIsEmpty = requestsQueue.isEmpty();
        final Long2LongHashMap routeIdsByReplyId = requestsQueue.computeIfAbsent(etag, this::createQueue);

        routeIdsByReplyId.put(acceptReplyId, acceptRouteId);

        if (requestQueueIsEmpty)
        {
            initiateRequest(etag, acceptRouteId, acceptReplyId);
        }
        else if (this.etag != null &&
                !this.etag.equals(etag))
        {
            resetInFightRequest();
            initiateRequest(null, acceptRouteId, acceptReplyId);
        }
    }

    void dequeue(
        String etag,
        long acceptReplyId)
    {
        final Long2LongHashMap routeIdsByReplyId = requestsQueue.get(etag);
        assert routeIdsByReplyId != null;

        final long acceptRouteId = routeIdsByReplyId.remove(acceptReplyId);
        assert acceptRouteId != routeIdsByReplyId.missingValue();
        requestsWithAnswer.removeLong(acceptReplyId);

        if (routeIdsByReplyId.isEmpty())
        {
            requestsQueue.remove(etag);

            if (requestsQueue.isEmpty())
            {
                cleaner.accept(requestHash);
            }
        }

        if (!requestsQueue.isEmpty())
        {
            MutableLong activeRouteId = new MutableLong();
            MutableLong activeReplyId = new MutableLong();

            if (!routeIdsByReplyId.isEmpty())
            {
                routeIdsByReplyId.forEach((replyId, routeId) ->
                {
                    if (!requestsWithAnswer.containsLong(replyId) &&
                        this.acceptReplyId == acceptReplyId)
                    {
                        activeRouteId.value = routeId;
                        activeReplyId.value = replyId;
                    }
                });
            }
            else
            {
                requestsQueue.forEach((key, value) ->
                {
                    value.forEach((replyId, routeId) ->
                    {
                        if (!requestsWithAnswer.containsLong(replyId) &&
                            this.acceptReplyId == acceptReplyId)
                        {
                            activeRouteId.value = routeId;
                            activeReplyId.value = replyId;
                        }
                    });
                });
            }

            if (activeRouteId.value != 0L)
            {
                resetInFightRequest();
                writer.doSignal(activeRouteId.value,
                                activeReplyId.value,
                                factory.supplyTrace.getAsLong(),
                                INITIATE_REQUEST_SIGNAL);
            }
        }
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

    void onGroupRequestReset()
    {
        factory.writer.doSignal(acceptRouteId,
                                acceptReplyId,
                                factory.supplyTrace.getAsLong(),
                                GROUP_REQUEST_RESET_SIGNAL);
        acceptRouteId = 0L;
        acceptReplyId = 0L;
    }

    MessageConsumer newRequest(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        BeginFW begin = factory.beginRO.wrap(buffer, index, length);
        routeId = begin.streamId();
        replyId = factory.supplyReplyId.applyAsLong(begin.streamId());
        connect = new HttpCacheProxyGroupRequest(factory, this)::onRequestMessage;
        return connect;
    }

    private void initiateRequest(
        String etag,
        long acceptRouteId,
        long acceptReplyId)
    {
        this.etag = etag;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        INITIATE_REQUEST_SIGNAL);
    }

    private void doSignalCacheEntryAborted(
        long acceptReplyId,
        long acceptRouteId)
    {
        gotResponse(acceptReplyId);
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        CACHE_ENTRY_ABORTED_SIGNAL);
    }

    private void gotResponse(long acceptReplyId)
    {
        if (!requestsWithAnswer.containsLong(acceptReplyId))
        {
            requestsWithAnswer.pushLong(acceptReplyId);
        }
    }

    private void doSignalCacheEntryUpdated(
        long acceptReplyId,
        long acceptRouteId)
    {
        gotResponse(acceptReplyId);
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTrace.getAsLong(),
                        CACHE_ENTRY_UPDATED_SIGNAL);

    }

    private void doSignalCacheEntryNotModified(
        long acceptReplyId,
        long acceptRouteId)
    {
        if (!requestsWithAnswer.containsLong(acceptReplyId))
        {
            requestsWithAnswer.pushLong(acceptReplyId);
            writer.doSignal(acceptRouteId,
                            acceptReplyId,
                            factory.supplyTrace.getAsLong(),
                            CACHE_ENTRY_NOT_MODIFIED_SIGNAL);
        }
    }

    private Long2LongHashMap createQueue(
        String etag)
    {
        Long2LongHashMap queue =  requestsQueue.get(etag);
        return Objects.requireNonNullElseGet(queue, () -> new Long2LongHashMap(-1));
    }

    private void resetInFightRequest()
    {
        factory.writer.doReset(connect,
                               routeId,
                               replyId,
                               factory.supplyTrace.getAsLong());
    }
}
