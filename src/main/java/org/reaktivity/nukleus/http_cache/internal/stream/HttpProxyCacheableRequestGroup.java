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
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_INVALIDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_NOT_MODIFIED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_GROUP_LEADER_UPDATED_SIGNAL;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;

public final class HttpProxyCacheableRequestGroup
{
    private final Map<String, Long2LongHashMap> queuedRequestsByEtag;
    private final LongHashSet responsesInFlight;
    private final Writer writer;
    private final HttpCacheProxyFactory factory;
    private final Consumer<Integer> cleaner;
    private final MutableLong activeRouteId;
    private final MutableLong activeReplyId;
    private final int requestHash;

    private MessageConsumer connect;
    private String etag;
    private String recentAuthorizationToken;
    private String vary;
    private long acceptReplyId;
    private long connectRouteId;
    private long connectId;

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
        this.queuedRequestsByEtag = new HashMap<>();
        this.responsesInFlight = new LongHashSet();
        this.activeRouteId = new MutableLong();
        this.activeReplyId = new MutableLong();
    }

    String getRecentAuthorizationToken()
    {
        return recentAuthorizationToken;
    }

    String getEtag()
    {
        return etag;
    }

    int getRequestHash()
    {
        return requestHash;
    }

    void setRecentAuthorizationToken(
        String recentAuthorizationToken)
    {
        this.recentAuthorizationToken = recentAuthorizationToken;
    }

    int getQueuedRequests()
    {
        MutableInteger totalRequests = new MutableInteger();
        queuedRequestsByEtag.forEach((key, routeIdsByReplyId) -> totalRequests.value += routeIdsByReplyId.size());
        return totalRequests.value;
    }

    boolean isRequestGroupLeader(
        long acceptReplyId)
    {
        return this.acceptReplyId == acceptReplyId;
    }

    boolean isRequestStillQueued(
        long replyId)
    {
        return connectId == replyId;
    }

    void enqueue(
        String etag,
        String newVary,
        long acceptRouteId,
        long acceptReplyId)
    {
        final boolean requestQueueIsEmpty = queuedRequestsByEtag.isEmpty();
        final Long2LongHashMap routeIdsByReplyId = queuedRequestsByEtag.computeIfAbsent(etag, this::createQueue);

        routeIdsByReplyId.put(acceptReplyId, acceptRouteId);

        if (requestQueueIsEmpty)
        {
            initiateRequest(etag, newVary, acceptRouteId, acceptReplyId);
        }
        else
        {
            if (this.etag != null &&
                !this.etag.equals(etag))
            {
                resetInFlightRequest();
                initiateRequest(null, newVary, acceptRouteId, acceptReplyId);
            }
            else if (vary != null && !vary.equalsIgnoreCase(newVary))
            {
                resetInFlightRequest();
                initiateRequest(etag, newVary, acceptRouteId, acceptReplyId);
            }
        }
    }

    void dequeue(
        String etag,
        long acceptReplyId)
    {
        final Long2LongHashMap routeIdsByReplyId = queuedRequestsByEtag.get(etag);
        assert routeIdsByReplyId != null;

        final long acceptRouteId = routeIdsByReplyId.remove(acceptReplyId);
        assert acceptRouteId != routeIdsByReplyId.missingValue();
        responsesInFlight.remove(acceptReplyId);

        if (routeIdsByReplyId.isEmpty())
        {
            queuedRequestsByEtag.remove(etag);

            if (queuedRequestsByEtag.isEmpty())
            {
                cleaner.accept(requestHash);
            }
        }

        if (!queuedRequestsByEtag.isEmpty())
        {
            resetActiveRequestValue();

            if (!routeIdsByReplyId.isEmpty())
            {
                routeIdsByReplyId.forEach((replyId, routeId) ->
                {
                    if (!responsesInFlight.contains(replyId) &&
                        this.acceptReplyId == acceptReplyId)
                    {
                        activeRouteId.value = routeId;
                        activeReplyId.value = replyId;
                    }
                });
            }
            else
            {
                queuedRequestsByEtag.forEach((key, value) ->
                {
                    value.forEach((replyId, routeId) ->
                    {
                        if (!responsesInFlight.contains(replyId) &&
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
                resetInFlightRequest();
                writer.doSignal(activeRouteId.value,
                                activeReplyId.value,
                                factory.supplyTraceId.getAsLong(),
                                REQUEST_GROUP_LEADER_UPDATED_SIGNAL);
            }
        }
        else if (isRequestGroupLeader(acceptReplyId))
        {
            resetActiveRequestValue();
        }
    }

    void onCacheableResponseUpdated(
        String etag)
    {
        queuedRequestsByEtag.forEach((key, routeIdsByReplyId) ->
        {
            if (key != null && key.equals(etag) ||
                (key == null && etag == null) && queuedRequestsByEtag.size() > 1)
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
        queuedRequestsByEtag.forEach((key, routeIdsByReplyId) ->
        {
            routeIdsByReplyId.forEach(this::doSignalCacheEntryAborted);
        });
    }

    public void onCacheEntryInvalidated()
    {
        writer.doSignal(connect,
                        connectRouteId,
            connectId,
                        factory.supplyTraceId.getAsLong(),
                        CACHE_ENTRY_INVALIDATED_SIGNAL);
    }

    MessageConsumer newRequest(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        BeginFW begin = factory.beginRO.wrap(buffer, index, length);
        connectRouteId = begin.routeId();
        connectId = begin.streamId();
        connect = new HttpCacheProxyGroupRequest(factory, this, sender)::onRequestMessage;
        return connect;
    }

    private void initiateRequest(
        String etag,
        String newVary,
        long acceptRouteId,
        long acceptReplyId)
    {
        this.etag = etag;
        this.vary = newVary;
        this.acceptReplyId = acceptReplyId;
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTraceId.getAsLong(),
                        REQUEST_GROUP_LEADER_UPDATED_SIGNAL);
    }

    private void doSignalCacheEntryAborted(
        long acceptReplyId,
        long acceptRouteId)
    {
        gotResponse(acceptReplyId);
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTraceId.getAsLong(),
                        CACHE_ENTRY_ABORTED_SIGNAL);
    }

    private void gotResponse(long acceptReplyId)
    {
        responsesInFlight.add(acceptReplyId);
    }

    private void doSignalCacheEntryUpdated(
        long acceptReplyId,
        long acceptRouteId)
    {
        gotResponse(acceptReplyId);
        writer.doSignal(acceptRouteId,
                        acceptReplyId,
                        factory.supplyTraceId.getAsLong(),
                        CACHE_ENTRY_UPDATED_SIGNAL);

    }

    private void doSignalCacheEntryNotModified(
        long acceptReplyId,
        long acceptRouteId)
    {
        if (!responsesInFlight.contains(acceptReplyId))
        {
            responsesInFlight.add(acceptReplyId);
            writer.doSignal(acceptRouteId,
                            acceptReplyId,
                            factory.supplyTraceId.getAsLong(),
                            CACHE_ENTRY_NOT_MODIFIED_SIGNAL);
        }
    }

    private Long2LongHashMap createQueue(
        String etag)
    {
        Long2LongHashMap queue =  queuedRequestsByEtag.get(etag);
        return Objects.requireNonNullElseGet(queue, () -> new Long2LongHashMap(-1));
    }

    private void resetInFlightRequest()
    {
        factory.writer.doReset(connect,
                               connectRouteId,
                               connectId,
                               factory.supplyTraceId.getAsLong());
    }

    private void resetActiveRequestValue()
    {
        activeRouteId.value = 0L;
        activeReplyId.value = 0L;
        connectRouteId = 0L;
        connectId = 0L;
    }
}
