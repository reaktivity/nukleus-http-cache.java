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

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.isCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_ABORTED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.INITIATE_REQUEST_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.AUTHORIZATION;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.time.Instant;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

final class HttpCacheProxyCacheableRequest
{
    private static final StringFW HEADER_NAME_STATUS = new StringFW(":status");
    private static final String16FW HEADER_VALUE_STATUS_503 = new String16FW("503");

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;
    private final int initialWindow;

    private MessageConsumer connectInitial;
    private MessageConsumer connectReply;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private final MutableInteger requestSlot;
    private final String requestURL;
    private final int requestHash;

    private boolean isRequestPurged;
    private String ifNoneMatch;
    private Future<?> preferWaitExpired;
    private int attempts;

    private int acceptReplyBudget;
    private long budgetId;
    private int padding;
    private int payloadWritten = -1;
    private DefaultCacheEntry cacheEntry;
    private boolean etagSent;
    private boolean requestQueued;
    private boolean requestExpired;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        int requestHash,
        String requestURL,
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
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connectInitial = connectInitial;
        this.connectReply = connectReply;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
        this.initialWindow = factory.responseBufferPool.slotCapacity();
        this.requestSlot =  new MutableInteger(NO_SLOT);
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        assert !isRequestPurged;

        MessageConsumer newStream;
        ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();
        boolean retry = HttpHeadersUtil.retry(responseHeaders);

        if ((retry && attempts < 3) ||
            this.factory.defaultCache.checkToRetry(getRequestHeaders(),
                                                   responseHeaders,
                                                   ifNoneMatch,
                                                   requestHash))
        {
            final HttpCacheProxyRetryResponse cacheProxyRetryResponse =
                new HttpCacheProxyRetryResponse(factory,
                                                requestHash,
                                                connectReply,
                                                connectRouteId,
                                                connectReplyId,
                                                this::scheduleRequest);
            newStream = cacheProxyRetryResponse::onResponseMessage;

        }
        else if (factory.defaultCache.matchCacheableResponse(requestHash,
                                                             getHeader(responseHeaders, ETAG),
                                                             getRequestHeaders().anyMatch(HAS_IF_NONE_MATCH)))
        {
            final HttpCacheProxyNotModifiedResponse notModifiedResponse =
                new HttpCacheProxyNotModifiedResponse(factory,
                                                      requestHash,
                                                      requestURL,
                                                      acceptReply,
                                                      acceptRouteId,
                                                      acceptReplyId,
                                                      connectReply,
                                                      connectReplyId,
                                                      connectRouteId,
                                                      getHeader(getRequestHeaders(), PREFER)
                );
            factory.router.setThrottle(acceptReplyId, notModifiedResponse::onResponseMessage);
            newStream = notModifiedResponse::onResponseMessage;
            cleanupRequestIfNecessary();
            requestGroup.onNonCacheableResponse(acceptReplyId);
            resetRequestTimeoutIfNecessary();
        }
        else if (isCacheableResponse(responseHeaders))
        {
            final HttpCacheProxyCacheableResponse cacheableResponse =
                new HttpCacheProxyCacheableResponse(factory,
                                                    requestGroup,
                                                    requestHash,
                                                    requestURL,
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
                                                           requestHash,
                                                           requestURL,
                                                           connectReply,
                                                           connectRouteId,
                                                           connectReplyId,
                                                           acceptReply,
                                                           acceptRouteId,
                                                           acceptReplyId);
            factory.router.setThrottle(acceptReplyId, nonCacheableResponse::onResponseMessage);
            newStream = nonCacheableResponse::onResponseMessage;
            requestGroup.onNonCacheableResponse(acceptReplyId);
            cleanupRequestIfNecessary();
            resetRequestTimeoutIfNecessary();
        }

        return newStream;
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
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
        final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

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

        HttpHeaderFW ifNoneMatchHeader = requestHeaders.matchFirst(h -> IF_NONE_MATCH.equals(h.name().asString()));
        if (ifNoneMatchHeader != null)
        {
            ifNoneMatch = ifNoneMatchHeader.value().asString();
            schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
        }

        if (requestGroup.queue(acceptRouteId, acceptReplyId))
        {
            ifNoneMatch = ifNoneMatchHeader.value().asString();
            schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
            factory.writer.doWindow(acceptReply,
                                    acceptRouteId,
                                    acceptInitialId,
                                    begin.traceId(),
                                    0L,
                                    initialWindow,
                                    0);
            requestQueued =  true;
            return;
        }
        doHttpBegin(requestHeaders);
    }

    private void onData(
        final DataFW data)
    {
        factory.writer.doWindow(acceptReply,
                                acceptRouteId,
                                acceptInitialId,
                                data.traceId(),
                                data.budgetId(),
                                data.reserved(),
                                0);
    }

    private void onEnd(
        final EndFW end)
    {
        if (!requestQueued)
        {
            final long traceId = end.traceId();
            factory.writer.doHttpEnd(connectInitial, connectRouteId, connectInitialId, traceId);
        }
    }

    private void onAbort(
        final AbortFW abort)
    {
        if (!requestQueued)
        {
            final long traceId = abort.traceId();
            factory.writer.doAbort(connectInitial, connectRouteId, connectInitialId, traceId);
        }
        cleanupRequestIfNecessary();
        resetRequestTimeoutIfNecessary();
    }

    private void onRequestWindow(
        final WindowFW window)
    {
        if (requestQueued)
        {
            factory.writer.doHttpEnd(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTraceId.getAsLong());
        }
        else
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();
            factory.writer.doWindow(acceptReply, acceptRouteId, acceptInitialId, traceId, budgetId, credit, padding);
        }
    }

    private void onRequestReset(
        final ResetFW reset)
    {
        final long traceId = reset.traceId();
        factory.writer.doReset(acceptReply, acceptRouteId, acceptInitialId, traceId);
        resetRequestTimeoutIfNecessary();
        cleanupRequestIfNecessary();
    }

    private void doHttpBegin(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        long connectReplyId = factory.supplyReplyId.applyAsLong(connectInitialId);

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTraceId.getAsLong(),
                                     mutateRequestHeaders(requestHeaders));
        factory.router.setThrottle(connectInitialId, this::onRequestMessage);

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [sent initial request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }
    }

    private void schedulePreferWaitIfNoneMatchIfNecessary(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = this.factory.executor.schedule(Math.min(preferWait, factory.preferWaitMaximum),
                                                                   SECONDS,
                                                                   this.acceptRouteId,
                                                                   this.acceptReplyId,
                                                                   REQUEST_EXPIRED_SIGNAL);
            }
        }
    }

    private boolean scheduleRequest(
        long retryAfter)
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
        ArrayFW<HttpHeaderFW> requestHeaders = getRequestHeaders();

        if (DEBUG)
        {
            System.out.printf("[%016x] CONNECT %016x %s [retry cacheable request]\n",
                              currentTimeMillis(), connectReplyId, getRequestURL(requestHeaders));
        }

        factory.writer.doHttpRequest(connectInitial,
                                     connectRouteId,
                                     connectInitialId,
                                     factory.supplyTraceId.getAsLong(),
                                     mutateRequestHeaders(requestHeaders));
        factory.counters.requestsRetry.getAsLong();
        factory.router.setThrottle(connectInitialId, this::onRequestMessage);
        requestQueued = true;
    }

    private boolean storeRequest(
        final ArrayFW<HttpHeaderFW> headers)
    {
        assert requestSlot.value == NO_SLOT;
        int newRequestSlot = factory.requestBufferPool.acquire(acceptInitialId);
        if (newRequestSlot == NO_SLOT)
        {
            return false;
        }
        requestSlot.value = newRequestSlot;
        MutableDirectBuffer requestCacheBuffer = factory.requestBufferPool.buffer(requestSlot.value);
        requestCacheBuffer.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
        return true;
    }

    private ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        final MutableDirectBuffer buffer = factory.requestBufferPool.buffer(requestSlot.value);
        return factory.requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }


    private void purge()
    {
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }
        isRequestPurged = true;
    }

    private void incAttempts()
    {
        attempts++;
    }

    private Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutateRequestHeaders(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        return (ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder) ->
        {
            requestHeaders.forEach(h ->
            {
                final String name = h.name().asString();
                final String value = h.value().asString();
                if (!CONTENT_LENGTH.equals(name) &&
                    !AUTHORIZATION.equals(name))
                {
                    builder.item(item -> item.name(name).value(value));
                }
            });

            final String authorizationToken = requestGroup.getRecentAuthorizationToken();
            if (authorizationToken != null)
            {
                builder.item(item -> item.name(AUTHORIZATION).value(authorizationToken));
            }
        };
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
            DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestHash);
            factory.defaultCache.send304(cacheEntry,
                                         getHeader(getRequestHeaders(), PREFER),
                                         acceptReply,
                                         acceptRouteId,
                                         acceptReplyId);
            cleanupRequestIfNecessary();
            Function<HttpBeginExFW, MessageConsumer> correlation = factory.correlations.remove(connectReplyId);
            if (correlation != null)
            {
                factory.router.clearThrottle(connectReplyId);
            }
            requestGroup.onNonCacheableResponse(acceptReplyId);
            requestExpired = true;
        }
        else if (signalId == CACHE_ENTRY_ABORTED_SIGNAL)
        {
            if (this.payloadWritten >= 0)
            {
                factory.writer.doAbort(acceptReply,
                                       acceptRouteId,
                                       acceptReplyId,
                                       signal.traceId());
                requestGroup.unqueue(acceptReplyId);
            }
            else
            {
                send503RetryAfter();
            }
            cleanupRequestIfNecessary();
            cacheEntry.setSubscribers(-1);
        }
        else if (signalId == INITIATE_REQUEST_SIGNAL)
        {
            doHttpBegin(getRequestHeaders());
        }
    }

    private void onResponseWindow(
        WindowFW window)
    {
        if (requestExpired)
        {
            factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTraceId.getAsLong());
        }
        else
        {
            budgetId = window.budgetId();
            padding = window.padding();
            long streamId = window.streamId();
            int credit = window.credit();
            acceptReplyBudget += credit;
            factory.budgetManager.window(BudgetManager.StreamKind.CACHE,
                                         budgetId,
                                         streamId,
                                         credit,
                                         this::writePayload,
                                         window.traceId());
            sendEndIfNecessary(window.traceId());
        }
    }

    private void onResponseReset(
        ResetFW reset)
    {
        factory.budgetManager.closed(BudgetManager.StreamKind.CACHE,
                                     budgetId,
                                     acceptReplyId,
                                     factory.supplyTraceId.getAsLong());
        factory.writer.doReset(acceptReply,
                               acceptRouteId,
                               acceptInitialId,
                               factory.supplyTraceId.getAsLong());
        cleanupRequestIfNecessary();
        requestGroup.onNonCacheableResponse(acceptReplyId);
        if (cacheEntry != null)
        {
            cacheEntry.setSubscribers(-1);
        }
        factory.correlations.remove(connectReplyId);
        resetRequestTimeoutIfNecessary();
    }

    private void send503RetryAfter()
    {
        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(),
                              acceptReplyId, "503");
        }

        factory.writer.doHttpResponse(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTraceId.getAsLong(),
            e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                  .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(
            acceptReply,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTraceId.getAsLong());

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
        requestGroup.unqueue(acceptReplyId);
        purge();
    }

    private void handleCacheUpdateSignal(
        SignalFW signal)
    {
        cacheEntry = factory.defaultCache.get(requestHash);
        if (cacheEntry == null)
        {
            cleanupRequestIfNecessary();
            send503RetryAfter();
            return;
        }
        if (payloadWritten == -1)
        {
            resetRequestTimeoutIfNecessary();
            sendHttpResponseHeaders(cacheEntry, signal.signalId());
        }
        else
        {
            factory.budgetManager.resumeAssigningBudget(budgetId, 0, signal.traceId());
            sendEndIfNecessary(signal.traceId());
        }
    }

    private void sendHttpResponseHeaders(
        DefaultCacheEntry cacheEntry,
        long signalId)
    {
        ArrayFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        if (DEBUG)
        {
            System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId,
                              getHeader(responseHeaders, ":status"));
        }

        boolean isStale = false;
        if (signalId == CACHE_ENTRY_SIGNAL)
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
                                                        factory.supplyTraceId.getAsLong());

        payloadWritten = 0;

        factory.counters.responses.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {

        boolean ackedBudget = !factory.budgetManager.hasUnackedBudget(budgetId, acceptReplyId);

        if (payloadWritten == cacheEntry.responseSize() &&
            ackedBudget &&
            cacheEntry.isResponseCompleted())
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
                                         budgetId,
                                         acceptReplyId,
                                         traceId);
            cleanupRequestIfNecessary();
            cacheEntry.setSubscribers(-1);
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
            factory.writer.doHttpData(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                trace,
                budgetId,
                toWrite + padding,
                p -> buildResponsePayload(payloadWritten,
                                          toWrite,
                                          p,
                                          factory.defaultCache.getResponsePool()));
            payloadWritten += toWrite;
            budget -= toWrite + padding;
            acceptReplyBudget -= toWrite + padding;
            assert acceptReplyBudget >= 0;
        }

        return budget;
    }

    private void buildResponsePayload(
        int index,
        int length,
        OctetsFW.Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    private void buildResponsePayload(
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
