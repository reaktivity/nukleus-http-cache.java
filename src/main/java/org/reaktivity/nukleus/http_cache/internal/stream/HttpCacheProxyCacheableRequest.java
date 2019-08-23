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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

import java.time.Instant;
import java.util.concurrent.Future;

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.ABORT_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

final class HttpCacheProxyCacheableRequest
{
    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;

    private MessageConsumer connectInitial;
    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private int requestSlot = NO_SLOT;
    private int requestHash;
    private boolean isRequestPurged;
    private String ifNoneMatch;
    private Future<?> preferWaitExpired;
    private int attempts;

    private int acceptReplyBudget;
    private long groupId;
    private int padding;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private boolean requestQueued;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptInitialId,
        long acceptReplyId,
        MessageConsumer connectInitial,
        MessageConsumer connectReply,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectInitial = connectInitial;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        ListFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);
        MessageConsumer newStream;

        if ((retry  &&
             attempts < 3) ||
            !this.factory.defaultCache.isUpdatedByResponseHeadersToRetry(getRequestHeaders(),
                                                                         responseHeaders,
                                                                         ifNoneMatch,
                                                                         requestHash))
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                connectReply,
                                                connectRouteId,
                                                connectReplyId,
                                                this::scheduleRequest);
            newStream = cacheProxyRetryResponse::onResponseMessage;

        }
        else if (factory.defaultCache.matchCacheableResponse(requestHash, getHeader(responseHeaders, ETAG)))
        {
            final HttpCacheProxyNotModifiedResponse notModifiedResponse =
                new HttpCacheProxyNotModifiedResponse(factory,
                                                      requestHash,
                                                      requestSlot,
                                                      acceptReply,
                                                      acceptRouteId,
                                                      acceptReplyId,
                                                      connectReply,
                                                      connectReplyId,
                                                      connectRouteId);
            newStream = notModifiedResponse::onResponseMessage;
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestGroup,
                                                    requestHash,
                                                    requestSlot,
                                                    acceptReply,
                                                    acceptRouteId,
                                                    acceptReplyId,
                                                    connectReply,
                                                    connectReplyId,
                                                    connectRouteId,
                                                    ifNoneMatch,
                                                    this::scheduleRequest);
            newStream = cacheableResponse::onResponseMessage;
        }
        else
        {
            final HttpCacheProxyNonCacheableResponse nonCacheableResponse =
                    new HttpCacheProxyNonCacheableResponse(factory,
                                                           connectReply,
                                                           connectRouteId,
                                                           connectReplyId,
                                                           acceptReply,
                                                           acceptRouteId,
                                                           acceptReplyId);
            factory.router.setThrottle(acceptReplyId, nonCacheableResponse::onResponseMessage);
            newStream = nonCacheableResponse::onResponseMessage;
            resetRequestTimeoutIfNecessary();
            cleanupRequestIfNecessary();
            requestGroup.serveNextIfPossible(requestHash, acceptReplyId);
        }

        return newStream;
    }

    boolean scheduleRequest(long retryAfter)
    {
        if (retryAfter <= 0L)
        {
            retryCacheableRequest();
        }
        else
        {
            long requestAt = Instant.now().plusMillis(retryAfter).toEpochMilli();
            this.factory.scheduler.accept(requestAt, this::retryCacheableRequest);
        }
        return true;
    }


    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch(msgTypeId)
        {
            case WindowFW.TYPE_ID:
                final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                onResponseWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
                onResponseSignal(signal);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
                onResponseReset(reset);
                break;
        }
    }

    private void doHttpRequest()
    {
        long connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders();
        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent initial request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(
            connectInitial,
            connectRouteId,
            connectInitialId,
            factory.supplyTrace.getAsLong(),
            builder -> requestHeaders.forEach(
                h ->  builder.item(item -> item.name(h.name()).value(h.value()))));

        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
    }

    void onRequestMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case BeginFW.TYPE_ID:
                final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = factory.endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                onRequestWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
                onRequestReset(reset);
                break;
            default:
                break;
        }
    }

    private void onBegin(
        BeginFW begin)
    {
        final long authorization = begin.authorization();
        final short authorizationScope = authorizationScope(authorization);

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [received request]\n",
                              currentTimeMillis(), acceptReplyId, getRequestURL(requestHeaders));
        }

        // count all requests
        factory.counters.requests.getAsLong();
        factory.counters.requestsCacheable.getAsLong();

        boolean stored = storeRequest(requestHeaders);
        if (!stored)
        {
            send503RetryAfter();
            return;
        }

        // Should already be canonicalized in http / http2 nuklei
        final String requestURL = getRequestURL(requestHeaders);
        this.requestHash = RequestUtil.requestHash(authorizationScope, requestURL.hashCode());

        HttpHeaderFW ifNoneMatch = requestHeaders.matchFirst(h -> IF_NONE_MATCH.equals(h.name().asString()));
        if (ifNoneMatch != null)
        {
            schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
            this.ifNoneMatch = ifNoneMatch.value().asString();
        }

        if (requestGroup.queue(requestHash, acceptReplyId, acceptRouteId))
        {
            requestQueued =  true;
            return;
        }

        doHttpRequest();
    }

    private void onData(
        final DataFW data)
    {
        //NOOP
    }

    private void onEnd(
        final EndFW end)
    {
        if (!requestQueued)
        {
            final long traceId = end.trace();
            factory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, traceId);
        }
    }

    private void onAbort(
        final AbortFW abort)
    {
        if (!requestQueued)
        {
            final long traceId = abort.trace();
            factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
        }
        cleanupRequestIfNecessary();
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();
        final long traceId = window.trace();
        factory.writer.doWindow(acceptReply, acceptRouteId, acceptInitialId, traceId, credit, padding, groupId);
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        final long traceId = reset.trace();
        factory.writer.doReset(acceptReply, acceptRouteId, acceptInitialId, traceId);

        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
        }

        requestGroup.unqueue(requestHash, acceptReplyId);
    }

    private void schedulePreferWaitIfNoneMatchIfNecessary(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = this.factory.executor.schedule(preferWait,
                                                                   SECONDS,
                                                                   acceptRouteId,
                                                                   this.acceptReplyId,
                                                                   REQUEST_EXPIRED_SIGNAL);
            }
        }
    }

    private void retryCacheableRequest()
    {
        if (isRequestPurged)
        {
            return;
        }

        incAttempts();

        connectInitialId = this.factory.supplyInitialId.applyAsLong(connectRouteId);
        connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);
        connectInitial = this.factory.router.supplyReceiver(connectInitialId);

        factory.correlations.put(connectReplyId, this::newResponse);
        ListFW<HttpHeaderFW> requestHeaders = getRequestHeaders();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTrace.getAsLong(),
                                     builder ->
                                     {
                                        requestHeaders.forEach(
                                            h -> builder.item(item -> item.name(h.name()).value(h.value())));
                                     });
        factory.writer.doHttpEnd(connectInitial,
                                 connectRouteId,
                                 connectInitialId,
                                 factory.supplyTrace.getAsLong());
        factory.counters.requestsRetry.getAsLong();
    }

    private boolean storeRequest(
        final ListFW<HttpHeaderFW> headers)
    {
        this.requestSlot = factory.requestBufferPool.acquire(acceptInitialId);
        if (requestSlot == NO_SLOT)
        {
            return false;
        }
        MutableDirectBuffer requestCacheBuffer = factory.requestBufferPool.buffer(requestSlot);
        requestCacheBuffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
        return true;
    }

    private ListFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }


    private void purge()
    {
        if (requestSlot != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot);
            this.requestSlot = NO_SLOT;
        }
        this.isRequestPurged = true;
    }

    private void incAttempts()
    {
        attempts++;
    }

    private void onResponseSignal(
        SignalFW signal)
    {
        final long signalId = signal.signalId();

        if (signalId == CACHE_ENTRY_UPDATED_SIGNAL)
        {
            handleCacheUpdateSignal(signal);
        }
        else if (signalId == REQUEST_EXPIRED_SIGNAL)
        {
            factory.defaultCache.send304(cacheEntry,
                                         getRequestHeaders(),
                                         acceptReply,
                                         acceptRouteId,
                                         acceptReplyId);
            factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
        }
        else if (signalId == ABORT_SIGNAL)
        {
            if (this.payloadWritten >= 0)
            {
                factory.writer.doAbort(acceptReply,
                                       acceptRouteId,
                                       acceptReplyId,
                                       signal.trace());
                 requestGroup.unqueue(requestHash, acceptReplyId);
                cleanupRequestIfNecessary();
            }
            else
            {
                send503RetryAfter();
            }
        }
    }

    private void onResponseWindow(WindowFW window)
    {
        groupId = window.groupId();
        padding = window.padding();
        long streamId = window.streamId();
        int credit = window.credit();
        acceptReplyBudget += credit;
        factory.budgetManager.window(BudgetManager.StreamKind.CACHE,
                                                   groupId,
                                                   streamId,
                                                   credit,
                                                   this::writePayload,
                                                   window.trace());
        sendEndIfNecessary(window.trace());
    }

    private void onResponseReset(ResetFW reset)
    {
        factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                     groupId,
                                     acceptReplyId,
                                     factory.supplyTrace.getAsLong());
        purge();
        factory.writer.doReset(acceptReply,
                               acceptRouteId,
                               acceptInitialId,
                               factory.supplyTrace.getAsLong());
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(),
                              acceptReplyId, "503");
        }

        factory.writer.doHttpResponse(acceptReply,
                                      acceptRouteId,
                                      acceptReplyId,
                                      factory.supplyTrace.getAsLong(), e ->
                                                        e.item(h -> h.name(STATUS).value(SERVICE_UNAVAILABLE_503))
                                                         .item(h -> h.name("retry-after").value("0")));
        factory.writer.doHttpEnd(acceptReply,
                                 acceptRouteId,
                                 acceptReplyId,
                                 factory.supplyTrace.getAsLong());

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();
    }

    private void resetRequestTimeoutIfNecessary()
    {
        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
        }
    }

    private void cleanupRequestIfNecessary()
    {
        requestGroup.unqueue(requestHash, acceptReplyId);
        purge();
    }

    private void handleCacheUpdateSignal(
        SignalFW signal)
    {
        cacheEntry = factory.defaultCache.get(requestHash);
        if (cacheEntry == null)
        {
            return;
        }
        if(payloadWritten == -1)
        {
            if (preferWaitExpired != null)
            {
                preferWaitExpired.cancel(true);
            }
            sendHttpResponseHeaders(cacheEntry, signal.signalId());
        }
        else
        {
            factory.budgetManager.resumeAssigningBudget(groupId, 0, signal.trace());
            sendEndIfNecessary(signal.trace());
        }
    }

    private void sendHttpResponseHeaders(
        DefaultCacheEntry cacheEntry,
        long signalId)
    {
        ListFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId,
                              getHeader(responseHeaders, ":status"));
        }

        boolean isStale = false;
        if(signalId == CACHE_ENTRY_SIGNAL)
        {
            isStale = cacheEntry.isStale();
        }

        if (cacheEntry.etag() != null)
        {
            etagSent = true;
        }

        factory.writer.doHttpResponseWithUpdatedHeaders(acceptReply,
                                                        acceptRouteId,
                                                        acceptReplyId,
                                                        responseHeaders,
                                                        cacheEntry.getRequestHeaders(),
                                                        cacheEntry.etag(),
                                                        isStale,

        factory.supplyTrace.getAsLong());

        payloadWritten = 0;

        factory.counters.responses.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {

        boolean ackedBudget = !factory.budgetManager.hasUnackedBudget(groupId, acceptReplyId);

        if (payloadWritten == cacheEntry.responseSize()
            && ackedBudget
            && cacheEntry.isResponseCompleted())
        {
            if (!etagSent && cacheEntry.etag() != null)
            {
                factory.writer.doHttpEnd(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       traceId,
                                                       cacheEntry.etag());
            }
            else
            {
                factory.writer.doHttpEnd(acceptReply,
                                                       acceptRouteId,
                                                       acceptReplyId,
                                                       traceId);
            }

            factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                                       groupId,
                                                       acceptReplyId,
                                                       traceId);
            cleanupRequestIfNecessary();
        }
    }

    private int writePayload(
        int budget,
        long trace)
    {
        final int minBudget = min(budget, acceptReplyBudget);
        final int toWrite = min(minBudget - padding, cacheEntry.responseSize() - payloadWritten);
        if (toWrite > 0)
        {
            factory.writer.doHttpData(acceptReply,
                                      acceptRouteId,
                                      acceptReplyId,
                                      trace,
                                      groupId,
                                      padding,
                                      p -> buildResponsePayload(payloadWritten,
                                          toWrite,
                                          p,
                                          cacheEntry.getResponsePool()));
            payloadWritten += toWrite;
            budget -= (toWrite + padding);
            acceptReplyBudget -= (toWrite + padding);
            assert acceptReplyBudget >= 0;
        }

        return budget;
    }

    public void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    public void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder builder,
        BufferPool bp,
        int slotCnt)
    {
        if (length == 0)
        {
            return;
        }

        final int slotCapacity = bp.slotCapacity();
        int chunkedWrite = (slotCnt * slotCapacity) - index;
        int slot = cacheEntry.getResponseSlots().get(slotCnt);
        if (chunkedWrite > 0)
        {
            MutableDirectBuffer buffer = bp.buffer(slot);
            int offset = slotCapacity - chunkedWrite;
            int chunkLength = Math.min(chunkedWrite, length);
            builder.put(buffer, offset, chunkLength);
            index += chunkLength;
            length -= chunkLength;
        }
        buildResponsePayload(index, length, builder, bp, ++slotCnt);
    }
}
