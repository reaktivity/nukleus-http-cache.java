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
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MAX_STALE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.MIN_FRESH;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives.S_MAXAGE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.sameAuthorizationScope;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.HttpStatus.NOT_MODIFIED_304;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayList;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

public final class DefaultCacheEntry
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    private final DefaultCache cache;
    private final int requestHash;
    private String etag;

    private Instant lazyInitiatedResponseReceivedAt;
    private Instant lazyInitiatedResponseStaleAt;

    private BufferPool requestPool;
    private int requestSlot = NO_SLOT;
    private int requestHeadersSize = 0;

    private BufferPool responsePool;
    private IntArrayList responseSlots = new IntArrayList();
    public static final int NUM_OF_HEADER_SLOTS = 1;
    private int responseHeadersSize = 0;
    private int responseSize = 0;
    private boolean responseCompleted = false;
    private int subscribers;

    DefaultCacheEntry(
        DefaultCache cache,
        int requestHash,
        BufferPool requestPool,
        BufferPool responsePool)
    {
        this.cache = cache;
        this.requestHash = requestHash;
        this.requestPool = requestPool;
        this.responsePool = responsePool;
    }

    public void setSubscribers(int numberOfSubscribers)
    {
        this.subscribers += numberOfSubscribers;
    }

    public int getSubscribers()
    {
        return subscribers;
    }

    public IntArrayList getResponseSlots()
    {
        return responseSlots;
    }

    public ListFW<HttpHeaderFW> getRequestHeaders()
    {
        return getRequestHeaders(cache.requestHeadersRO);
    }

    public ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO)
    {
        return getRequestHeaders(requestHeadersRO, requestPool);
    }

    public boolean storeRequestHeaders(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        final int slotCapacity = responsePool.slotCapacity();
        if (slotCapacity < requestHeaders.sizeof())
        {
            return false;
        }
        int requestHeaderSlot = requestPool.acquire(requestHash);
        if (requestHeaderSlot == NO_SLOT)
        {
            this.cache.purgeEntriesForNonPendingRequests();
            requestHeaderSlot = requestPool.acquire(requestHash);
            if (requestHeaderSlot == NO_SLOT)
            {
                return false;
            }
        }
        this.requestSlot = requestHeaderSlot;

        MutableDirectBuffer buffer = requestPool.buffer(requestSlot);
        buffer.putBytes(0, requestHeaders.buffer(), requestHeaders.offset(), requestHeaders.sizeof());
        this.requestHeadersSize = requestHeaders.sizeof();
        return true;
    }

    public ListFW<HttpHeaderFW> getCachedResponseHeaders()
    {
        return this.getResponseHeaders(cache.cachedResponseHeadersRO);
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(ListFW<HttpHeaderFW> responseHeadersRO)
    {
        return getResponseHeaders(responseHeadersRO, responsePool);
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO,
        BufferPool bp)
    {
        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = bp.buffer(firstResponseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public boolean storeResponseHeaders(
        ListFW<HttpHeaderFW> responseHeaders)
    {
        final int slotCapacity = responsePool.slotCapacity();
        if (slotCapacity < responseHeaders.sizeof())
        {
            return false;
        }
        etag = getHeader(responseHeaders, ETAG);
        int headerSlot = responsePool.acquire(requestHash);
        if (headerSlot == NO_SLOT)
        {
            this.cache.purgeEntriesForNonPendingRequests();
            headerSlot = responsePool.acquire(requestHash);
            if (headerSlot == NO_SLOT)
            {
                return false;
            }
        }
        responseSlots.add(headerSlot);

        MutableDirectBuffer buffer = responsePool.buffer(headerSlot);
        buffer.putBytes(0, responseHeaders.buffer(), responseHeaders.offset(), responseHeaders.sizeof());
        this.responseHeadersSize = responseHeaders.sizeof();

        return true;
    }

    public void updateResponseHeader(
        String status,
        ListFW<HttpHeaderFW> newHeaders)
    {
        final ListFW<HttpHeaderFW> responseHeadersSO = new HttpBeginExFW().headers();
        ListFW<HttpHeaderFW> oldHeaders = getResponseHeaders(responseHeadersSO);
        String statusCode = Objects.requireNonNull(oldHeaders.matchFirst(h -> Objects.requireNonNull(h.name().asString())
            .toLowerCase().equals(":status"))).value().asString();

        final LinkedHashMap<String, String> newHeadersMap = new LinkedHashMap<>();
        oldHeaders.forEach(h ->
            newHeadersMap.put(h.name().asString(), h.value().asString()));
        newHeaders.forEach(h ->
            newHeadersMap.put(h.name().asString(), h.value().asString()));
        newHeadersMap.put(":status", statusCode);

        if (NOT_MODIFIED_304.equals(status))
        {
            try
            {
                newHeadersMap.put(":date", DATE_FORMAT.format(Date.from(Instant.now())));
            }
            catch (Exception e)
            {
                //NOOP
            }
        }

        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = responsePool.buffer(firstResponseSlot);

        final ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headersRW =
            new ListFW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());

        this.responseHeadersSize = responseBuffer.capacity();
        headersRW.wrap(responseBuffer, 0, responseHeadersSize);

        for(Map.Entry<String, String> entry : newHeadersMap.entrySet())
        {
            headersRW.item(y -> y.name(entry.getKey()).value(entry.getValue()));
        }

        headersRW.build();
    }

    public boolean storeResponseData(
        DataFW data)
    {
        return storeResponseData(data.payload());
    }

    public int requestHeadersSize()
    {
        return requestHeadersSize;
    }

    public int responseSize()
    {
        return responseSize;
    }

    public void purge()
    {
        this.evictRequest();
        this.evictResponse();
    }

    public String etag()
    {
        return etag;
    }

    public void setEtag(String etag)
    {
        this.etag = etag;
    }

    public boolean isResponseCompleted()
    {
        return responseCompleted;
    }

    public void setResponseCompleted(boolean responseCompleted)
    {
        this.responseCompleted = responseCompleted;
    }

    public boolean canServeRequest(
        ListFW<HttpHeaderFW> request,
        short authScope)
    {
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

    public void evictRequest()
    {
        this.requestPool.release(requestSlot);
        this.requestSlot = NO_SLOT;
        this.requestPool = null;
    }

    public void evictResponse()
    {
        this.responseSlots.forEach(i -> responsePool.release(i));
        this.responseSlots.clear();
        this.responseSize = 0;
        this.setResponseCompleted(false);
    }

    public int requestHash()
    {
        return this.requestHash;
    }

    private boolean storeResponseData(
        Flyweight data)
    {
        return this.storeResponseData(responsePool, data, 0);
    }

    private boolean storeResponseData(
        BufferPool bp,
        Flyweight data,
        int written)
    {
        responsePool = bp;
        if (data.sizeof() - written == 0)
        {
            return true;
        }

        final int slotCapacity = bp.slotCapacity();
        int slotSpaceRemaining = (slotCapacity * (responseSlots.size() - NUM_OF_HEADER_SLOTS)) - responseSize;
        if (slotSpaceRemaining == 0)
        {
            slotSpaceRemaining = slotCapacity;
            int newSlot = bp.acquire(requestHash);
            if (newSlot == NO_SLOT)
            {
                this.cache.purgeEntriesForNonPendingRequests();
                newSlot = bp.acquire(requestHash);
                if (newSlot == NO_SLOT)
                {
                    return false;
                }
            }
            responseSlots.add(newSlot);
        }

        int toWrite = Math.min(slotSpaceRemaining, data.sizeof() - written);

        int slot = responseSlots.get(responseSlots.size() - NUM_OF_HEADER_SLOTS);

        MutableDirectBuffer buffer = bp.buffer(slot);
        buffer.putBytes(slotCapacity - slotSpaceRemaining, data.buffer(), data.offset() + written, toWrite);
        written += toWrite;
        responseSize += toWrite;
        return storeResponseData(bp, data, written);
    }

    private static String getHeader(ListFW<HttpHeaderFW> cachedRequestHeadersRO, String headerName)
    {
        // TODO remove GC when have streaming API: https://github.com/reaktivity/nukleus-maven-plugin/issues/16
        final StringBuilder header = new StringBuilder();
        cachedRequestHeadersRO.forEach(h ->
        {
            if (headerName.equalsIgnoreCase(h.name().asString()))
            {
                header.append(h.value().asString());
            }
        });

        return header.length() == 0 ? null : header.toString();
    }

    public boolean isStale()
    {
        return Instant.now().isAfter(staleAt());
    }


    private CacheControl responseCacheControl()
    {
        ListFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        String cacheControl = getHeader(responseHeaders, CACHE_CONTROL);
        return cache.responseCacheControlFW.parse(cacheControl);
    }

    private boolean doesNotVaryBy(ListFW<HttpHeaderFW> request)
    {
        final ListFW<HttpHeaderFW> responseHeaders = this.getCachedResponseHeaders();
        final ListFW<HttpHeaderFW> cachedRequest = getRequestHeaders(this.cache.requestHeadersRO);
        return CacheUtils.doesNotVary(request, responseHeaders, cachedRequest);
    }

    private boolean canBeServedToAuthorized(
        ListFW<HttpHeaderFW> request,
        short requestAuthScope)
    {
        final CacheControl responseCacheControl = responseCacheControl();
        final ListFW<HttpHeaderFW> cachedRequestHeaders = this.getRequestHeaders(this.cache.requestHeadersRO);
        return sameAuthorizationScope(request, cachedRequestHeaders, responseCacheControl);
    }

    private ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO,
        BufferPool bp)
    {
        final MutableDirectBuffer buffer = bp.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, buffer.capacity());
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
}
