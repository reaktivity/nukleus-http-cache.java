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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry.NUM_OF_HEADER_SLOTS;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.SERVICE_UNAVAILABLE_503;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferIfNoneMatch;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_ABORTED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_NOT_MODIFIED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.CACHE_ENTRY_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_EXPIRED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.Signals.REQUEST_GROUP_LEADER_UPDATED_SIGNAL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.RequestUtil.authorizationScope;

import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
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
    private static final String16FW HEADER_VALUE_STATUS_503 = new String16FW(SERVICE_UNAVAILABLE_503);

    private final HttpCacheProxyFactory factory;
    private final HttpProxyCacheableRequestGroup requestGroup;
    private final MutableInteger requestSlot;
    private final int initialWindow;

    private final MessageConsumer accept;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;

    private MessageConsumer connect;
    private long connectRouteId;
    private long connectReplyId;
    private long connectInitialId;

    private final String requestURL;
    private final int requestHash;

    private String ifNoneMatch;
    private String prefer;
    private Future<?> preferWaitExpired;

    private BudgetDebitor acceptReplyDebitor;
    private int acceptReplyBudget;
    private long acceptReplyDebitorId;
    private long acceptReplyDebitorIndex = NO_DEBITOR_INDEX;

    private DefaultCacheEntry cacheEntry;
    private long authorization;
    private int acceptReplyPadding;
    private int payloadWritten = -1;
    private short authScope;
    private boolean etagSent;
    private boolean responseClosing;
    private boolean isEmulatedProtocolStack;

    HttpCacheProxyCacheableRequest(
        HttpCacheProxyFactory factory,
        HttpProxyCacheableRequestGroup requestGroup,
        int requestHash,
        String requestURL,
        MessageConsumer accept,
        long acceptRouteId,
        long acceptInitialId,
        long acceptReplyId,
        MessageConsumer connect,
        long connectInitialId,
        long connectReplyId,
        long connectRouteId)
    {
        this.factory = factory;
        this.requestGroup = requestGroup;
        this.requestHash = requestHash;
        this.requestURL = requestURL;
        this.accept = accept;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptInitialId;
        this.acceptReplyId = acceptReplyId;
        this.connect = connect;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectInitialId = connectInitialId;
        this.initialWindow = factory.writeBuffer.capacity();
        this.requestSlot =  new MutableInteger(NO_SLOT);
    }

    MessageConsumer newResponse(
        HttpBeginExFW beginEx)
    {
        MessageConsumer newStream;
        ArrayFW<HttpHeaderFW> responseHeaders = beginEx.headers();

        if (factory.defaultCache.matchCacheableResponse(requestGroup.getRequestHash(),
                                                        getHeader(responseHeaders, ETAG),
                                                        ifNoneMatch != null))
        {
            newStream = new HttpCacheProxyNotModifiedResponse(factory,
                                                             requestHash,
                                                             authScope,
                                                             requestURL,
                                                             accept,
                                                             acceptRouteId,
                                                             acceptReplyId,
                                                             connect,
                                                             connectReplyId,
                                                             connectRouteId,
                                                             prefer)::onResponseMessage;
        }
        else
        {
            newStream = new HttpCacheProxyNonCacheableResponse(factory,
                                                               requestHash,
                                                               requestURL,
                                                               connect,
                                                               connectRouteId,
                                                               connectReplyId,
                                                               accept,
                                                               acceptRouteId,
                                                               acceptReplyId)::onResponseMessage;
            factory.defaultCache.purge(requestGroup.getRequestHash());
        }

        connect = factory.router.supplyReceiver(connectReplyId);
        factory.router.setThrottle(acceptReplyId, newStream);
        cleanupRequestSlotIfNecessary();
        cleanupRequestTimeoutIfNecessary();
        requestGroup.dequeue(ifNoneMatch, acceptReplyId);

        assert newStream != null;

        return newStream;
    }

    void onAcceptMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
            onRequestBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onRequestData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onRequestEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onRequestAbort(abort);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            onResponseReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            onResponseWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = factory.signalRO.wrap(buffer, index, index + length);
            onResponseSignal(signal);
            break;
        default:
            break;
        }
    }

    private void onRequestBegin(
        BeginFW begin)
    {
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();
        final DefaultCacheEntry cacheEntry = factory.defaultCache.get(requestGroup.getRequestHash());
        final long traceId = begin.traceId();
        authorization = begin.authorization();
        authScope = authorizationScope(authorization);
        isEmulatedProtocolStack = requestHeaders.anyMatch(HAS_EMULATED_PROTOCOL_STACK);

        boolean stored = storeRequest(requestHeaders);
        if (!stored)
        {
            send503RetryAfter();
            return;
        }

        ifNoneMatch = getHeader(requestHeaders, IF_NONE_MATCH);
        if (ifNoneMatch != null)
        {
            schedulePreferWaitIfNoneMatchIfNecessary(requestHeaders);
        }
        prefer = getHeader(requestHeaders, PREFER);

        final String vary = cacheEntry != null && cacheEntry.getVaryBy() != null ?
            getHeader(requestHeaders, cacheEntry.getVaryBy()) : null;

        requestGroup.enqueue(ifNoneMatch, vary, acceptRouteId, acceptReplyId);
        factory.writer.doWindow(accept,
                                acceptRouteId,
                                acceptInitialId,
                                traceId,
                                0L,
                                initialWindow,
                                0);

    }

    private void onRequestData(
        final DataFW data)
    {
        factory.writer.doWindow(accept,
                                acceptRouteId,
                                acceptInitialId,
                                data.traceId(),
                                data.budgetId(),
                                data.reserved(),
                                0);
    }

    private void onRequestEnd(
        final EndFW end)
    {
        //NOOP
    }

    private void onRequestAbort(
        final AbortFW abort)
    {
        final long traceId = abort.traceId();
        factory.writer.doAbort(connect, connectRouteId, connectInitialId, traceId);
        factory.writer.doReset(accept, acceptRouteId, acceptInitialId, traceId);

        cleanupRequestIfNecessary();
        cleanupRequestTimeoutIfNecessary();
        cleanupResponseIfNecessary();
    }

    private void schedulePreferWaitIfNoneMatchIfNecessary(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        if (isPreferIfNoneMatch(requestHeaders))
        {
            int preferWait = getPreferWait(requestHeaders);
            if (preferWait > 0)
            {
                preferWaitExpired = factory.executor.schedule(Math.min(preferWait, factory.preferWaitMaximum),
                                                              SECONDS,
                                                              acceptRouteId,
                                                              acceptReplyId,
                                                              REQUEST_EXPIRED_SIGNAL);
            }
        }
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

    private void onResponseSignal(
        SignalFW signal)
    {
        final int signalId = signal.signalId();

        switch (signalId)
        {
        case REQUEST_GROUP_LEADER_UPDATED_SIGNAL:
            onResponseSignalRequestGroupLeaderUpdated(signal);
            break;
        case REQUEST_EXPIRED_SIGNAL:
            onResponseSignalRequestExpired(signal);
            break;
        case CACHE_ENTRY_NOT_MODIFIED_SIGNAL:
            onResponseSignalCacheEntryNotModified(signal);
            break;
        case CACHE_ENTRY_UPDATED_SIGNAL:
            onResponseSignalCacheEntryUpdated(signal);
            break;
        case CACHE_ENTRY_ABORTED_SIGNAL:
            onResponseSignalCacheEntryAborted(signal);
            break;
        default:
            assert false : "Unsupported signal id";
        }
    }

    private void onResponseSignalCacheEntryUpdated(
        SignalFW signal)
    {
        cacheEntry = factory.defaultCache.get(requestGroup.getRequestHash());

        if (payloadWritten == -1)
        {
            if (!requestGroup.isRequestGroupLeader(acceptReplyId))
            {
                if (cacheEntry.getVaryBy() != null &&
                    (requestSlot.value == NO_SLOT || !cacheEntry.doesNotVaryBy(getRequestHeaders())))
                {
                    cleanupRequestIfNecessary();
                    send503RetryAfter();
                    return;
                }
            }
            cleanupRequestTimeoutIfNecessary();
            sendHttpResponseHeaders(cacheEntry);
        }
        else
        {
            final long traceId = signal.traceId();
            doResponseFlush(traceId);
        }
    }

    private void onResponseSignalRequestExpired(
        SignalFW signal)
    {
        factory.counters.responses.getAsLong();
        factory.defaultCache.send304(ifNoneMatch,
                                     prefer,
                                     accept,
                                     acceptRouteId,
                                     acceptReplyId);
        responseClosing = true;
    }

    private void onResponseSignalCacheEntryAborted(
        SignalFW signal)
    {
        if (payloadWritten >= 0)
        {
            factory.writer.doAbort(accept,
                                   acceptRouteId,
                                   acceptReplyId,
                                   signal.traceId());
            requestGroup.dequeue(ifNoneMatch, acceptReplyId);
            cacheEntry.setSubscribers(-1);
            cleanupResponseIfNecessary();
        }
        else
        {
            send503RetryAfter();
        }

        cleanupRequestIfNecessary();
    }

    private void onResponseSignalRequestGroupLeaderUpdated(
        SignalFW signal)
    {
        if (requestGroup.isRequestGroupLeader(acceptReplyId))
        {
            final ArrayFW<HttpHeaderFW> requestHeaders = getRequestHeaders();
            Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
                builder -> requestHeaders.forEach(h -> builder.item(item -> item.name(h.name()).value(h.value())));

            connect = factory.writer.newHttpStream(requestGroup::newRequest, connectRouteId, connectInitialId,
                factory.supplyTraceId.getAsLong(), mutator, this::onConnectMessage);

            factory.writer.doHttpRequest(connect,
                connectRouteId,
                connectInitialId,
                signal.traceId(),
                authorization,
                mutator);
            cleanupRequestSlotIfNecessary();
        }
    }

    private void onConnectMessage(
        int type,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (type == ResetFW.TYPE_ID)
        {
            ResetFW reset = factory.resetRO.wrap(buffer, index, length);
            onConnectResponseReset(reset);
        }
    }

    private void onConnectResponseReset(
        ResetFW reset)
    {
        factory.writer.doReset(accept, acceptRouteId, acceptInitialId, reset.traceId());
        cleanupRequestIfNecessary();
    }

    private void onResponseSignalCacheEntryNotModified(
        SignalFW signal)
    {
        factory.counters.responses.getAsLong();
        factory.defaultCache.send304(ifNoneMatch,
                                     prefer,
                                     accept,
                                     acceptRouteId,
                                     acceptReplyId);
        cleanupRequestTimeoutIfNecessary();
        responseClosing = true;
    }

    private void onResponseWindow(
        WindowFW window)
    {
        if (responseClosing)
        {
            if (isEmulatedProtocolStack)
            {
                final DefaultCacheEntry entry = factory.defaultCache.get(requestGroup.getRequestHash());
                if (entry != null)
                {
                    factory.counters.promises.getAsLong();

                    factory.writer.doHttpPushPromise(accept,
                        acceptRouteId,
                        acceptReplyId,
                        authScope,
                        entry.getRequestHeaders(),
                        entry.getCachedResponseHeaders(),
                        entry.etag());
                }
            }
            factory.writer.doHttpEnd(accept, acceptRouteId, acceptReplyId, factory.supplyTraceId.getAsLong());
            cleanupRequestIfNecessary();
            cleanupResponseIfNecessary();
        }
        else
        {
            acceptReplyDebitorId = window.budgetId();
            acceptReplyPadding = window.padding();
            final long traceId = window.traceId();
            final int credit = window.credit();
            acceptReplyBudget += credit;

            if (acceptReplyDebitorId != 0L && acceptReplyDebitor == null)
            {
                acceptReplyDebitor = factory.supplyDebitor.apply(acceptReplyDebitorId);
                acceptReplyDebitorIndex = acceptReplyDebitor.acquire(acceptReplyDebitorId, acceptReplyId, this::doResponseFlush);
            }

            doResponseFlush(traceId);
        }
    }

    private void onResponseReset(
        ResetFW reset)
    {
        cleanupRequestIfNecessary();
        cleanupResponseIfNecessary();
        if (cacheEntry != null)
        {
            cacheEntry.setSubscribers(-1);
        }
    }

    private void doResponseFlush(
        long traceId)
    {
        final int remaining = cacheEntry.responseSize() - payloadWritten;
        int writable = Math.min(acceptReplyBudget - acceptReplyPadding, remaining);

        if (writable > 0)
        {
            final int maximum = writable + acceptReplyPadding;
            final int minimum = Math.min(maximum, 1024);

            int claimed = maximum;
            if (acceptReplyDebitorIndex != NO_DEBITOR_INDEX)
            {
                claimed = acceptReplyDebitor.claim(acceptReplyDebitorIndex, acceptReplyId, minimum, maximum);
            }

            final int required = claimed;
            final int writableMax = required - acceptReplyPadding;
            if (writableMax > 0)
            {
                final BufferPool cacheResponsePool = factory.defaultCache.getResponsePool();

                factory.writer.doHttpData(
                    accept,
                    acceptRouteId,
                    acceptReplyId,
                    traceId,
                    acceptReplyDebitorId,
                    required,
                    p -> buildResponsePayload(payloadWritten, writableMax, p, cacheResponsePool));

                payloadWritten += writableMax;

                acceptReplyBudget -= required;
                assert acceptReplyBudget >= 0;
            }
        }

        sendEndIfNecessary(traceId);
    }

    private void send503RetryAfter()
    {
        factory.writer.doHttpResponse(
            accept,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTraceId.getAsLong(),
            e -> e.item(h -> h.name(HEADER_NAME_STATUS).value(HEADER_VALUE_STATUS_503))
                  .item(h -> h.name("retry-after").value("0")));

        factory.writer.doHttpEnd(
            accept,
            acceptRouteId,
            acceptReplyId,
            factory.supplyTraceId.getAsLong());

        cleanupResponseIfNecessary();

        // count all responses
        factory.counters.responses.getAsLong();

        // count retry responses
        factory.counters.responsesRetry.getAsLong();
    }

    private void sendHttpResponseHeaders(
        DefaultCacheEntry cacheEntry)
    {
        final ArrayFW<HttpHeaderFW> responseHeaders = cacheEntry.getCachedResponseHeaders();

        if (cacheEntry.etag() != null)
        {
            etagSent = true;
        }

        factory.writer.doHttpResponseWithUpdatedHeaders(accept,
                                                        acceptRouteId,
                                                        acceptReplyId,
                                                        responseHeaders,
                                                        cacheEntry.getRequestHeaders(),
                                                        cacheEntry.etag(),
                                                        false,
                                                        factory.supplyTraceId.getAsLong());


        payloadWritten = 0;

        factory.counters.responses.getAsLong();
    }

    private void sendEndIfNecessary(
        long traceId)
    {
        if (payloadWritten == cacheEntry.responseSize() &&
            cacheEntry.isResponseCompleted())
        {
            if (isEmulatedProtocolStack)
            {
                factory.counters.promises.getAsLong();
                factory.writer.doHttpPushPromise(accept,
                                                 acceptRouteId,
                                                 acceptReplyId,
                                                 authScope,
                                                 cacheEntry.getRequestHeaders(),
                                                 cacheEntry.getCachedResponseHeaders(),
                                                 cacheEntry.etag());
            }

            if (!etagSent &&
                cacheEntry.etag() != null)
            {
                factory.writer.doHttpEnd(accept,
                                         acceptRouteId,
                                         acceptReplyId,
                                         traceId,
                                         cacheEntry.etag());
            }
            else
            {
                factory.writer.doHttpEnd(accept,
                                         acceptRouteId,
                                         acceptReplyId,
                                         traceId);
            }

            cleanupRequestIfNecessary();
            cleanupResponseIfNecessary();
            cacheEntry.setSubscribers(-1);
        }
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
        final int chunkedWrite = (slotCnt * slotCapacity) - index;
        final int slot = cacheEntry.getResponseSlots().get(slotCnt);
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

    private void cleanupResponseIfNecessary()
    {
        if (acceptReplyDebitorIndex != NO_DEBITOR_INDEX)
        {
            acceptReplyDebitor.release(acceptReplyDebitorIndex, acceptReplyId);
            acceptReplyDebitorIndex = NO_DEBITOR_INDEX;
            acceptReplyDebitor = null;
        }
    }

    private void cleanupRequestTimeoutIfNecessary()
    {
        if (preferWaitExpired != null)
        {
            preferWaitExpired.cancel(true);
            preferWaitExpired = null;
        }
    }

    private void cleanupRequestIfNecessary()
    {
        requestGroup.dequeue(ifNoneMatch, acceptReplyId);
        cleanupRequestSlotIfNecessary();
        factory.router.clearThrottle(connectReplyId);
        factory.router.clearThrottle(acceptReplyId);
    }

    private void cleanupRequestSlotIfNecessary()
    {
        if (requestSlot.value != NO_SLOT)
        {
            factory.requestBufferPool.release(requestSlot.value);
            requestSlot.value = NO_SLOT;
        }
    }
}
