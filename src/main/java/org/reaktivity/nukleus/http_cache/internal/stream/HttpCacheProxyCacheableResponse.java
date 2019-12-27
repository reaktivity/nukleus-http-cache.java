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

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;

import java.time.Instant;
import java.util.function.LongConsumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCacheEntry;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpEndExFW;

final class HttpCacheProxyCacheableResponse
{
    private static final long NO_RETRY_AFTER = Long.MIN_VALUE;
    private final HttpCacheProxyFactory factory;
    private final HttpCacheProxyCacheableRequest request;
    private final HttpProxyCacheableRequestGroup requestGroup;

    private final MessageConsumer initial;
    private final long routeId;
    private final long replyId;
    private final DefaultCacheEntry cacheEntry;
    private final LongConsumer retryRequestAfter;
    private final Runnable cleanupRequest;

    private String ifNoneMatch;
    private int replyBudget;
    private Instant responseAt;
    private long retryAfter = NO_RETRY_AFTER;

    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory factory,
        HttpCacheProxyCacheableRequest request,
        MessageConsumer initial,
        long routeId,
        long replyId,
        DefaultCacheEntry cacheEntry,
        LongConsumer retryRequestAfter,
        Runnable cleanupRequest)
    {
        this.factory = factory;
        this.request = request;
        this.requestGroup = request.requestGroup;
        this.initial = initial;
        this.routeId = routeId;
        this.replyId = replyId;
        this.ifNoneMatch = requestGroup.ifNoneMatchHeader(); // can this be removed?
        this.cacheEntry = cacheEntry;
        this.retryRequestAfter = retryRequestAfter;
        this.cleanupRequest = cleanupRequest;
    }

    @Override
    public String toString()
    {
        return String.format("%s[routeId=%016x, replyId=%d, replyBudget=%d]",
                getClass().getSimpleName(), routeId, replyId, replyBudget);
    }

    void doResponseReset(
        long traceId)
    {
        factory.writer.doReset(initial, routeId, replyId, traceId);
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
            onResponseBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            onResponseData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = factory.endRO.wrap(buffer, index, index + length);
            onResponseEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
            onResponseAbort(abort);
            break;
        }
    }

    private void onResponseBegin(
        BeginFW begin)
    {
        final long traceId = begin.traceId();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginFW = extension.get(factory.httpBeginExRO::wrap);
        final ArrayFW<HttpHeaderFW> headers = httpBeginFW.headers();

        final boolean stored = cacheEntry.storeResponseHeaders(headers);
        assert stored;

        final Instant receivedAt = cacheEntry.receivedAt();
        final Instant now = Instant.now();
        responseAt = receivedAt.isBefore(now) ? receivedAt : now;
        requestGroup.cacheEntry(cacheEntry);

        final boolean hasEtagHeader = cacheEntry.etag() != null;
        if (hasEtagHeader &&
            factory.defaultCache.checkTrailerToRetry(ifNoneMatch,
                                                     cacheEntry))
        {
            retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
        }

        if (hasEtagHeader && retryAfter == NO_RETRY_AFTER)
        {
            requestGroup.onGroupResponseUpdated(responseAt, traceId, ifNoneMatch);
        }

        doResponseWindow(traceId, factory.initialWindowSize);
    }

    private void onResponseData(
        DataFW data)
    {
        final long traceId = data.traceId();
        final int reserved = data.reserved();

        doResponseWindow(traceId, reserved);

        boolean stored = cacheEntry.storeResponseData(data);
        assert stored;

        final boolean hasEtagHeader = cacheEntry.etag() != null;
        if (hasEtagHeader && retryAfter == NO_RETRY_AFTER)
        {
            requestGroup.onGroupResponseUpdated(responseAt, traceId, ifNoneMatch);
        }
    }

    private void onResponseEnd(
        EndFW end)
    {
        final long traceId = end.traceId();
        final OctetsFW extension = end.extension();
        final HttpEndExFW httpEndEx = extension.get(factory.httpEndExRO::tryWrap);
        final boolean hasEtagHeader = cacheEntry.etag() != null;

        if (httpEndEx != null)
        {
            ArrayFW<HttpHeaderFW> trailers = httpEndEx.trailers();
            HttpHeaderFW etag = trailers.matchFirst(h -> ETAG.equals(h.name().asString()));
            if (etag != null)
            {
                String newEtag = etag.value().asString();
                cacheEntry.setEtag(newEtag);
            }
        }
        cacheEntry.setResponseCompleted(true);

        if (!hasEtagHeader &&
            factory.defaultCache.checkTrailerToRetry(ifNoneMatch,
                                                     cacheEntry))
        {
            retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
        }

        if (retryAfter != NO_RETRY_AFTER)
        {
            retryRequestAfter.accept(retryAfter);
        }
        else
        {
            cleanupRequest.run();
            requestGroup.onGroupResponseUpdated(responseAt, traceId, ifNoneMatch);
            requestGroup.onGroupRequestComplete(request);
        }
    }

    private void onResponseAbort(
        AbortFW abort)
    {
        final int requestHash = requestGroup.requestHash();
        factory.defaultCache.purge(requestHash);

        final long traceId = abort.traceId();
        cleanupRequest.run();
        requestGroup.onGroupResponseAborted(request, traceId);
        requestGroup.onGroupRequestComplete(request);
    }

    private void doResponseWindow(
        long traceId,
        int credit)
    {
        replyBudget += credit;
        if (replyBudget > 0)
        {
            factory.writer.doWindow(initial,
                                    routeId,
                                    replyId,
                                    traceId,
                                    0L,
                                    credit,
                                    0);
        }
    }
}
