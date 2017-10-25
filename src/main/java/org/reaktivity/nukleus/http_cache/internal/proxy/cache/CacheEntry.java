/**
 * Copyright 2016-2017 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_STALE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MIN_FRESH;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.S_MAXAGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.sameAuthorizationScope;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.WARNING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class CacheEntry
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    private final Cache cache;
    private final int requestSlot;
    private final int requestSize;
    private final int responseSlot;
    private final int responseHeaderSize;
    private final int responseSize;
    private int clientCount = 0;
    private boolean cleanUp = false;

    private Instant lazyInitiatedResponseReceivedAt;
    private Instant lazyInitiatedResponseStaleAt;

    private CacheControl cacheControlFW;

    CacheEntry(
        Cache cache,
        int requestSlot,
        int requestSize,
        int responseSlot,
        int responseHeaderSize,
        int responseSize)
    {
        this.cache = cache;
        this.requestSlot = requestSlot;
        this.requestSize = requestSize;
        this.responseSlot = responseSlot;
        this.responseHeaderSize = responseHeaderSize;
        this.responseSize = responseSize;
    }

    private void handleEndOfStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        this.removeClient();
    }

    // forwards response to client once cached (No need to inject warnings)
    public void forwardToClient(CacheableRequest streamCorrelation)
    {
        sendResponseToClient(streamCorrelation, false);
    }

    // serves client from cache (need to inject warnings
    public void serveClient(
            CacheableRequest streamCorrelation)
    {
        sendResponseToClient(streamCorrelation, true);
    }

    private void sendResponseToClient(
        CacheableRequest request,
        boolean injectWarnings)
    {
        addClient();
        MutableDirectBuffer buffer = this.cache.requestBufferPool.buffer(responseSlot);
        ListFW<HttpHeaderFW> responseHeaders = this.cache.responseHeadersRO.wrap(buffer, 0, responseHeaderSize);

        ServeFromCacheStream serveFromCacheStream = new ServeFromCacheStream(
                request,
                responseSlot,
                responseHeaderSize,
                responseSize,
                this::handleEndOfStream);
        request.setThrottle(serveFromCacheStream);


        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = x -> responseHeaders
                .forEach(h ->
                        x.item(y -> y.representation((byte) 0)
                                .name(h.name())
                                .value(h.value())));

        if (injectWarnings && isStale())
        {
            headers = headers.andThen(
                    x ->  x.item(h -> h.representation((byte) 0).name(WARNING).value(Cache.RESPONSE_IS_STALE))
            );
        }
        final MessageConsumer acceptReply = request.acceptReply();
        long acceptReplyStreamId = request.acceptReplyStreamId();
        long acceptReplyRef = request.acceptRef();
        long acceptCorrelationId = request.acceptCorrelationId();
        this.cache.writer.doHttpBegin(acceptReply, acceptReplyStreamId, acceptReplyRef, acceptCorrelationId, headers);
        final ListFW<HttpHeaderFW> requestHeaders = request.getRequestHeaders(cache.requestHeadersRO);
        int freshnessExtension = SurrogateControl.getMaxAgeFreshnessExtension(responseHeaders, cacheControlFW);
        if (freshnessExtension > 0)
        {
            this.cache.writer.doHttpPushPromise(request, requestHeaders, responseHeaders, freshnessExtension);
        }
    }

    public void cleanUp()
    {
        cleanUp = true;
        if (clientCount == 0)
        {
            this.cache.responseBufferPool.release(responseSlot);
            this.cache.requestBufferPool.release(requestSlot);
        }
    }

    class ServeFromCacheStream implements MessageConsumer
    {
        private final Request request;
        private int payloadWritten;
        private int responseSlot;
        private int responseHeaderSize;
        private int responseSize;
        private MessageConsumer onEnd;

         ServeFromCacheStream(
            Request request,
            int responseSlot,
            int responseHeaderSize,
            int responseSize,
            MessageConsumer onEnd)
        {
            this.payloadWritten = 0;
            this.request = request;
            this.responseSlot = responseSlot;
            this.responseHeaderSize = responseHeaderSize;
            this.responseSize = responseSize - responseHeaderSize;
            this.onEnd = onEnd;
        }

        @Override
        public void accept(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch(msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = CacheEntry.this.cache.windowRO.wrap(buffer, index, index + length);
                    int update = window.update();
                    writePayload(update);
                    break;
                case ResetFW.TYPE_ID:
                default:
                    this.onEnd.accept(msgTypeId, buffer, index, length);
                    break;
            }
        }

        private void writePayload(int update)
        {
            final int toWrite = Math.min(update, responseSize - payloadWritten);
            if (toWrite > 0)
            {
                final int offset = responseHeaderSize + payloadWritten;
                MutableDirectBuffer buffer = CacheEntry.this.cache.responseBufferPool.buffer(responseSlot);

                final MessageConsumer acceptReply = request.acceptReply();
                final long acceptReplyStreamId = request.acceptReplyStreamId();

                CacheEntry.this.cache.writer.doHttpData(acceptReply, acceptReplyStreamId, buffer, offset, toWrite);
                payloadWritten += toWrite;
                if (payloadWritten == responseSize)
                {
                    CacheEntry.this.cache.writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                    this.onEnd.accept(EndFW.TYPE_ID, buffer, offset, toWrite);
                }
            }
        }
    }

    private ListFW<HttpHeaderFW> getRequest()
    {
        DirectBuffer buffer = this.cache.requestBufferPool.buffer(requestSlot);
        return this.cache.requestHeadersRO.wrap(buffer, 0, requestSize);
    }

    public ListFW<HttpHeaderFW> getResponseHeaders()
    {
        DirectBuffer buffer = this.cache.responseBufferPool.buffer(responseSlot);
        return this.cache.responseHeadersRO.wrap(buffer, 0, responseHeaderSize);
    }

//  TODO remove
//    public OctetsFW getResponse(OctetsFW octetsFW)
//    {
//        DirectBuffer buffer = this.cache.responseBufferPool.buffer(responseSlot);
//        return octetsFW.wrap(buffer, responseHeaderSize, responseSize);
//    }

    public void addClient()
    {
        this.clientCount++;
    }

    public void removeClient()
    {
        clientCount--;
        if (clientCount == 0 && cleanUp)
        {
            this.cache.responseBufferPool.release(responseSlot);
            this.cache.requestBufferPool.release(requestSlot);
        }
    }

    private boolean canBeServedToAuthorized(
        ListFW<HttpHeaderFW> request)
    {
        final CacheControl responseCacheControl = responseCacheControl();
        final ListFW<HttpHeaderFW> cachedRequestHeaders = this.getRequest();
        return sameAuthorizationScope(request, cachedRequestHeaders, responseCacheControl);
    }

    private boolean doesNotVaryBy(ListFW<HttpHeaderFW> request)
    {
        final ListFW<HttpHeaderFW> responseHeaders = this.getResponseHeaders();
        final ListFW<HttpHeaderFW> cachedRequest = getRequest();
        return CacheUtils.doesNotVary(request, responseHeaders, cachedRequest);
    }


    private boolean satisfiesFreshnessRequirementsOf(
            ListFW<HttpHeaderFW> request,
            Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.requestCacheControlParser.parse(requestCacheControlHeaderValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MIN_FRESH))
        {
            final String minFresh = requestCacheControl.getValue(MIN_FRESH);
            if (! now.plusSeconds(parseInt(minFresh)).isBefore(staleAt))
            {
                return false;
            }
        }
        return true;
    }

    private boolean satisfiesStalenessRequirementsOf(
            ListFW<HttpHeaderFW> request,
            Instant now)
    {
        final String requestCacheControlHeacerValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.requestCacheControlParser.parse(requestCacheControlHeacerValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MAX_STALE))
        {
            final String maxStale = requestCacheControl.getValue(MAX_STALE);
            final int maxStaleSec = (maxStale != null) ? parseInt(maxStale): MAX_VALUE;
            final Instant acceptable = staleAt.plusSeconds(maxStaleSec);
            if (now.isAfter(acceptable))
            {
                return false;
            }
        }
        else if (now.isAfter(staleAt))
        {
            return false;
        }

        return true;
    }

    private boolean satisfiesAgeRequirementsOf(
        ListFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.requestCacheControlParser.parse(requestCacheControlHeaderValue);
        Instant receivedAt = responseReceivedAt();

        if (requestCacheControl.contains(MAX_AGE))
        {
            int requestMaxAge = parseInt(requestCacheControl.getValue(MAX_AGE));
            if (receivedAt.plusSeconds(requestMaxAge).isBefore(now))
            {
                return false;
            }
        }
        return true;
    }

    private Instant staleAt()
    {
        if (lazyInitiatedResponseStaleAt == null)
        {
            CacheControl cacheControl = responseCacheControl();
            Instant receivedAt = responseReceivedAt();
            int staleInSeconds = cacheControl.contains(S_MAXAGE) ?
                parseInt(cacheControl.getValue(S_MAXAGE))
                : cacheControl.contains(MAX_AGE) ?  parseInt(cacheControl.getValue(MAX_AGE)) : 0;
            lazyInitiatedResponseStaleAt = receivedAt.plusSeconds(staleInSeconds);
        }
        return lazyInitiatedResponseStaleAt;
    }

    private Instant responseReceivedAt()
    {
        if (lazyInitiatedResponseReceivedAt == null)
        {
            final ListFW<HttpHeaderFW> responseHeaders = getResponseHeaders();
            final String dateHeaderValue = getHeader(responseHeaders, "date") != null ?
                    getHeader(responseHeaders, "date") : getHeader(responseHeaders, "last-modified");
            try
            {
                Date receivedDate = DATE_FORMAT.parse(dateHeaderValue);
                lazyInitiatedResponseReceivedAt = receivedDate.toInstant();
            }
            catch (Exception e)
            {
                lazyInitiatedResponseReceivedAt = Instant.EPOCH;
            }
        }
        return lazyInitiatedResponseReceivedAt;
    }


    private CacheControl responseCacheControl()
    {
        ListFW<HttpHeaderFW> responseHeaders = getResponseHeaders();
        String cacheControl = getHeader(responseHeaders, CACHE_CONTROL);
        return cache.responseCacheControlParser.parse(cacheControl);
    }

    public boolean canServeRequest(
        int requestURLHash,
        ListFW<HttpHeaderFW> request,
        boolean isRevalidating)
    {
        Instant now = Instant.now();

        return canBeServedToAuthorized(request)
               && doesNotVaryBy(request)
               && satisfiesFreshnessRequirementsOf(request, now)
               && (satisfiesStalenessRequirementsOf(request, now) || isRevalidating)
               && satisfiesAgeRequirementsOf(request, now);
    }

    private boolean isStale()
    {
        return Instant.now().isAfter(staleAt());
    }

}
