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
    private final HttpCacheProxyFactory factory;
    private final HttpCacheProxyCacheableRequest request;
    private final HttpProxyCacheableRequestGroup requestGroup;

    private final MessageConsumer initial;
    private final long routeId;
    private final long replyId;
    private final DefaultCacheEntry cacheEntry;
    private final LongConsumer scheduleRetryAfter;

    private String ifNoneMatch;
    private int replyBudget;

    HttpCacheProxyCacheableResponse(
        HttpCacheProxyFactory factory,
        HttpCacheProxyCacheableRequest request,
        MessageConsumer initial,
        long routeId,
        long replyId,
        DefaultCacheEntry cacheEntry,
        LongConsumer scheduleRetryAfter)
    {
        this.factory = factory;
        this.request = request;
        this.requestGroup = request.requestGroup;
        this.initial = initial;
        this.routeId = routeId;
        this.replyId = replyId;
        this.ifNoneMatch = requestGroup.ifNoneMatchHeader(); // can this be removed?
        this.scheduleRetryAfter = scheduleRetryAfter;
        this.cacheEntry = cacheEntry;
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

        requestGroup.cacheEntry(cacheEntry);

        // TODO: notify request group immediately if not too early

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

        // TODO: notify request group immediately if not too early
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

        requestGroup.onCacheableResponseUpdated(traceId, ifNoneMatch);

        if (hasEtagHeader &&
            factory.defaultCache.checkTrailerToRetry(ifNoneMatch,
                                                     cacheEntry))
        {
            long retryAfter = HttpHeadersUtil.retryAfter(cacheEntry.getCachedResponseHeaders());
            scheduleRetryAfter.accept(retryAfter);
        }
        else
        {
            requestGroup.onGroupRequestComplete(request);
        }
    }

    private void onResponseAbort(
        AbortFW abort)
    {
        final long traceId = abort.traceId();
        requestGroup.onCacheableResponseAborted(request, traceId);
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
