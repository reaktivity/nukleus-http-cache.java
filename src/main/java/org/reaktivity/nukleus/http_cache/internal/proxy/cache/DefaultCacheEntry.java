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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration.DEBUG;
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
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.InitialRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.BudgetManager;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class DefaultCacheEntry
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    private final CacheControl cacheControlFW = new CacheControl();

    private final DefaultCache cache;
    private int clientCount;

    private Instant lazyInitiatedResponseReceivedAt;
    private Instant lazyInitiatedResponseStaleAt;

    final CacheableRequest cachedRequest;

    private CacheEntryState state;

    private final LongSupplier supplyTrace;

    public DefaultCacheEntry(
        DefaultCache cache,
        CacheableRequest request,
        LongSupplier supplyTrace)
    {
        this.cache = cache;
        this.cachedRequest = request;
        this.state = CacheEntryState.INITIALIZED;
        this.supplyTrace = requireNonNull(supplyTrace);
    }

    private void handleEndOfStream()
    {
        this.removeClient();
    }

    public void serveClient(
        AnswerableByCacheRequest streamCorrelation)
    {
        switch (this.state)
        {
            case PURGED:
                throw new IllegalStateException("Can not serve client when entry is purged");
            default:
                sendResponseToClient(streamCorrelation, true);
                break;
        }
        streamCorrelation.purge();
    }

    private void sendResponseToClient(
        AnswerableByCacheRequest request,
        boolean injectWarnings)
    {
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();

        ServeFromCacheStream serveFromCacheStream = new ServeFromCacheStream(
            request,
            cachedRequest,
            this::handleEndOfStream);
        request.setThrottle(serveFromCacheStream);

        final MessageConsumer acceptReply = request.acceptReply();
        long acceptRouteId = request.acceptRouteId();
        long acceptReplyId = request.acceptReplyId();

        // TODO should reduce freshness extension by how long it has aged
        int freshnessExtension = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        if (freshnessExtension > 0)
        {
            if (DEBUG)
            {
                System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId,
                    getHeader(responseHeaders, ":status"));
            }

            this.cache.writer.doHttpResponseWithUpdatedCacheControl(
                acceptReply,
                acceptRouteId,
                acceptReplyId,
                cacheControlFW,
                responseHeaders,
                freshnessExtension,
                cachedRequest.etag(),
                cachedRequest.authorizationHeader(),
                supplyTrace.getAsLong());

        }
        else
        {
            Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = x -> responseHeaders
                .forEach(h -> x.item(y -> y.name(h.name()).value(h.value())));

            // TODO inject stale on above if (freshnessExtension > 0)?
            if (injectWarnings && isStale())
            {
                headers = headers.andThen(
                    x ->  x.item(h -> h.name(WARNING).value(DefaultCache.RESPONSE_IS_STALE))
                );
            }

            if (DEBUG)
            {
                System.out.printf("[%016x] ACCEPT %016x %s [sent response]\n", currentTimeMillis(), acceptReplyId,
                    getHeader(responseHeaders, ":status"));
            }

            this.cache.writer.doHttpResponse(acceptReply, acceptRouteId, acceptReplyId, supplyTrace.getAsLong(), headers);
        }

        // count all responses
        cache.counters.responses.getAsLong();

        // count cached responses (cache hits)
        if (request instanceof InitialRequest)
        {
            // matching with requestsCacheable (which accounts only InitialRequest)
            cache.counters.responsesCached.getAsLong();
        }
    }

    public void purge()
    {
        switch (this.state)
        {
            case PURGED:
                break;
            default:
                this.state = CacheEntryState.PURGED;
                cachedRequest.purge();
                break;
        }
    }

    void recentAuthorizationHeader(String authorizationHeader)
    {
        if (authorizationHeader != null)
        {
            cachedRequest.recentAuthorizationHeader(authorizationHeader);
        }
    }

    class ServeFromCacheStream implements MessageConsumer
    {
        private final Request request;
        private int payloadWritten;
        private final Runnable onEnd;
        private CacheableRequest cachedRequest;
        private long groupId;
        private int padding;
        private int acceptReplyBudget;

        ServeFromCacheStream(
            Request request,
            CacheableRequest cachedRequest,
            Runnable onEnd)
        {
            this.payloadWritten = 0;
            this.request = request;
            this.cachedRequest = cachedRequest;
            this.onEnd = onEnd;
        }

        public void accept(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch(msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = cache.windowRO.wrap(buffer, index, index + length);
                    groupId = window.groupId();
                    padding = window.padding();
                    long streamId = window.streamId();
                    int credit = window.credit();
                    acceptReplyBudget += credit;
                    cache.budgetManager.window(BudgetManager.StreamKind.CACHE, groupId, streamId, credit,
                        this::writePayload, window.trace());

                    boolean ackedBudget = !cache.budgetManager.hasUnackedBudget(groupId, streamId);
                    if (payloadWritten == cachedRequest.responseSize() && ackedBudget)
                    {
                        final MessageConsumer acceptReply = request.acceptReply();
                        final long acceptRouteId = request.acceptRouteId();
                        final long acceptReplyStreamId = request.acceptReplyId();
                        DefaultCacheEntry.this.cache.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyStreamId,
                            window.trace());
                        this.onEnd.run();
                        cache.budgetManager.closed(BudgetManager.StreamKind.CACHE, groupId, acceptReplyStreamId, window.trace());
                    }
                    break;
                case ResetFW.TYPE_ID:
                default:
                    cache.budgetManager.closed(BudgetManager.StreamKind.CACHE, groupId, request.acceptReplyId(),
                        supplyTrace.getAsLong());
                    this.onEnd.run();
                    break;
            }
        }

        private int writePayload(int budget, long trace)
        {
            final MessageConsumer acceptReply = request.acceptReply();
            final long acceptRouteId = request.acceptRouteId();
            final long acceptReplyStreamId = request.acceptReplyId();

            final int minBudget = min(budget, acceptReplyBudget);
            final int toWrite = min(minBudget - padding, this.cachedRequest.responseSize() - payloadWritten);
            if (toWrite > 0)
            {
                cache.writer.doHttpData(
                    acceptReply,
                    acceptRouteId,
                    acceptReplyStreamId,
                    trace,
                    0L,
                    padding,
                    p -> cachedRequest.buildResponsePayload(payloadWritten, toWrite, p, cache.cachedResponseBufferPool)
                );
                payloadWritten += toWrite;
                budget -= (toWrite + padding);
                acceptReplyBudget -= (toWrite + padding);
                assert acceptReplyBudget >= 0;
            }

            return budget;
        }
    }

    private ListFW<HttpHeaderFW> getCachedRequest()
    {
        return cachedRequest.getRequestHeaders(cache.cachedRequestHeadersRO);
    }

    public ListFW<HttpHeaderFW> getCachedResponseHeaders()
    {
        return cachedRequest.getResponseHeaders(cache.cachedResponseHeadersRO);
    }

    protected ListFW<HttpHeaderFW> getCachedResponseHeaders(ListFW<HttpHeaderFW> responseHeadersRO, BufferPool bp)
    {
        return cachedRequest.getResponseHeaders(responseHeadersRO, bp);
    }

    private void removeClient()
    {
        clientCount--;
        if (clientCount == 0 && this.state == CacheEntryState.PURGED)
        {
            cachedRequest.purge();
        }
    }

    private boolean canBeServedToAuthorized(
        ListFW<HttpHeaderFW> request,
        short requestAuthScope)
    {

        if (SurrogateControl.isProtectedEx(this.getCachedResponseHeaders()))
        {
            return requestAuthScope == cachedRequest.authScope();
        }

        final CacheControl responseCacheControl = responseCacheControl();
        final ListFW<HttpHeaderFW> cachedRequestHeaders = this.getCachedRequest();
        return sameAuthorizationScope(request, cachedRequestHeaders, responseCacheControl);
    }

    private boolean doesNotVaryBy(ListFW<HttpHeaderFW> request)
    {
        final ListFW<HttpHeaderFW> responseHeaders = this.getCachedResponseHeaders();
        final ListFW<HttpHeaderFW> cachedRequest = getCachedRequest();
        return CacheUtils.doesNotVary(request, responseHeaders, cachedRequest);
    }

    // Checks this entry's vary header with the given entry's vary header
    public boolean doesNotVaryBy(DefaultCacheEntry entry)
    {
        final ListFW<HttpHeaderFW> thisHeaders = this.getCachedResponseHeaders();
        final ListFW<HttpHeaderFW> entryHeaders = entry.getCachedResponseHeaders(
            cache.cachedResponse1HeadersRO, cache.cachedResponse1BufferPool);
        assert thisHeaders.buffer() != entryHeaders.buffer();

        String thisVary = HttpHeadersUtil.getHeader(thisHeaders, HttpHeaders.VARY);
        String entryVary = HttpHeadersUtil.getHeader(entryHeaders, HttpHeaders.VARY);
        boolean varyMatches = (thisVary == entryVary) || (thisVary != null && thisVary.equalsIgnoreCase(entryVary));
        if (varyMatches)
        {
            final ListFW<HttpHeaderFW> requestHeaders = entry.cachedRequest.getRequestHeaders(
                cache.cachedRequest1HeadersRO, cache.cachedRequest1BufferPool);
            return doesNotVaryBy(requestHeaders);
        }
        return false;
    }


    private boolean satisfiesFreshnessRequirementsOf(
        ListFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeaderValue);

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
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeacerValue);

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
        final CacheControl requestCacheControl = cache.cachedRequestCacheControlFW.parse(requestCacheControlHeaderValue);

        if (requestCacheControl.contains(MAX_AGE))
        {
            int requestMaxAge = parseInt(requestCacheControl.getValue(MAX_AGE));
            Instant receivedAt = responseReceivedAt();
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
            int surrogateAge = SurrogateControl.getSurrogateAge(this.getCachedResponseHeaders());
            staleInSeconds = Math.max(staleInSeconds, surrogateAge);
            lazyInitiatedResponseStaleAt = receivedAt.plusSeconds(staleInSeconds);
        }
        return lazyInitiatedResponseStaleAt;
    }

    private Instant responseReceivedAt()
    {
        if (lazyInitiatedResponseReceivedAt == null)
        {
            final ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
            final String dateHeaderValue = getHeader(responseHeaders, HttpHeaders.DATE) != null ?
                getHeader(responseHeaders, HttpHeaders.DATE) : getHeader(responseHeaders, HttpHeaders.LAST_MODIFIED);
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
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        String cacheControl = getHeader(responseHeaders, CACHE_CONTROL);
        return cache.responseCacheControlFW.parse(cacheControl);
    }


    public boolean canServeRequest(
        ListFW<HttpHeaderFW> request,
        short authScope)
    {
        if (this.state == CacheEntryState.PURGED)
        {
            return false;
        }
        Instant now = Instant.now();

        final boolean canBeServedToAuthorized = canBeServedToAuthorized(request, authScope);
        final boolean doesNotVaryBy = doesNotVaryBy(request);
        final boolean satisfiesFreshnessRequirements = satisfiesFreshnessRequirementsOf(request, now);
        final boolean satisfiesStalenessRequirements = satisfiesStalenessRequirementsOf(request, now);
        final boolean satisfiesAgeRequirements = satisfiesAgeRequirementsOf(request, now);
        return canBeServedToAuthorized &&
            doesNotVaryBy &&
            satisfiesFreshnessRequirements &&
            satisfiesStalenessRequirements &&
            satisfiesAgeRequirements;
    }

    private boolean isStale()
    {
        return Instant.now().isAfter(staleAt());
    }

    protected boolean isIntendedForSingleUser()
    {
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        if (SurrogateControl.isProtectedEx(responseHeaders))
        {
            return false;
        }
        else
        {
            // TODO pull out as utility of CacheUtils
            String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.CACHE_CONTROL);
            return cacheControl != null && cache.responseCacheControlFW.parse(cacheControl).contains(CacheDirectives.PRIVATE);
        }
    }

    public boolean isUpdatedBy(CacheableRequest request)
    {
        ListFW<HttpHeaderFW> responseHeaders = request.getResponseHeaders(cache.responseHeadersRO);
        String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
        String etag = request.etag();
        boolean etagMatches = false;
        if (etag != null && this.cachedRequest.etag() !=  null)
        {
            etagMatches = status.equals(HttpStatus.OK_200) && this.cachedRequest.etag().equals(etag);
        }

        assert status != null;
        boolean notModified = status.equals(HttpStatus.NOT_MODIFIED_304) || etagMatches;

        return !notModified;
    }

    public boolean isSelectedForUpdate(CacheableRequest request)
    {
        ListFW<HttpHeaderFW> responseHeaders = request.getResponseHeaders(cache.responseHeadersRO);
        String status = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.STATUS);
        String etag = HttpHeadersUtil.getHeader(responseHeaders, HttpHeaders.ETAG);

        assert status != null;

        return status.equals(HttpStatus.NOT_MODIFIED_304) &&
            this.cachedRequest.etag().equals(etag);
    }

    public int requestUrl()
    {
        return this.cachedRequest.requestURLHash();
    }

}