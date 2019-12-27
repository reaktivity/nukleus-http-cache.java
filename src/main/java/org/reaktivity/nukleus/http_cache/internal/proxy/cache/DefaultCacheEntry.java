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
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl.getSurrogateAge;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

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
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

public final class DefaultCacheEntry
{
    public static final int NUM_OF_HEADER_SLOTS = 1;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    private final BufferPool requestPool;
    private final BufferPool responsePool;
    private final IntArrayList responseSlots;

    private final DefaultCache cache;
    private final int requestHash;
    private final int requestHashWithoutQuery;
    private final short authScope;

    private String etag;
    private String varyBy;
    private int requestSlot = NO_SLOT;
    private int responseHeadersSize;
    private int responseSize;
    private int subscribers;
    private boolean validationRequired;
    private boolean responseCompleted;
    private Instant cacheStaleAt;
    private Instant cacheReceivedAt;

    DefaultCacheEntry(
        DefaultCache cache,
        int requestHash,
        short authScope,
        int requestHashWithoutQuery,
        BufferPool requestPool,
        BufferPool responsePool)
    {
        this.cache = cache;
        this.requestHash = requestHash;
        this.authScope = authScope;
        this.requestHashWithoutQuery = requestHashWithoutQuery;
        this.requestPool = requestPool;
        this.responsePool = responsePool;
        this.responseSlots = new IntArrayList();
    }

    public String getVaryBy()
    {
        return varyBy;
    }

    public int requestHash()
    {
        return requestHash;
    }

    public int requestHashWithoutQuery()
    {
        return requestHashWithoutQuery;
    }

    public int responseSize()
    {
        return responseSize;
    }

    public String etag()
    {
        return etag;
    }

    public void setEtag(
        String etag)
    {
        this.etag = etag;
    }

    public boolean isResponseCompleted()
    {
        return responseCompleted;
    }

    public void setResponseCompleted(
        boolean responseCompleted)
    {
        if (responseCompleted)
        {
            validationRequired = false;
        }

        this.responseCompleted = responseCompleted;
    }

    public void invalidate()
    {
        validationRequired = true;
    }

    public void setSubscribers(
        int numberOfSubscribers)
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

    public ArrayFW<HttpHeaderFW> getRequestHeaders()
    {
        return getRequestHeaders(cache.requestHeadersRO);
    }

    public ArrayFW<HttpHeaderFW> getRequestHeaders(
        ArrayFW<HttpHeaderFW> requestHeadersRO)
    {
        return getRequestHeaders(requestHeadersRO, requestPool);
    }

    public boolean storeRequestHeaders(
        ArrayFW<HttpHeaderFW> requestHeaders)
    {
        evictRequestIfNecessary();
        final int slotCapacity = responsePool.slotCapacity();
        if (slotCapacity < requestHeaders.sizeof())
        {
            return false;
        }
        int requestHeaderSlot = requestPool.acquire(requestHash);
        if (requestHeaderSlot == NO_SLOT)
        {
            cache.purgeEntriesForNonPendingRequests();
            requestHeaderSlot = requestPool.acquire(requestHash);
            if (requestHeaderSlot == NO_SLOT)
            {
                return false;
            }
        }
        requestSlot = requestHeaderSlot;

        MutableDirectBuffer buffer = requestPool.buffer(requestSlot);
        buffer.putBytes(0, requestHeaders.buffer(), requestHeaders.offset(), requestHeaders.sizeof());
        return true;
    }

    public ArrayFW<HttpHeaderFW> getCachedResponseHeaders()
    {
        return this.getResponseHeaders(cache.cachedResponseHeadersRO);
    }

    public ArrayFW<HttpHeaderFW> getResponseHeaders(
        ArrayFW<HttpHeaderFW> responseHeadersRO)
    {
        return getResponseHeaders(responseHeadersRO, responsePool);
    }

    public ArrayFW<HttpHeaderFW> getResponseHeaders(
        ArrayFW<HttpHeaderFW> responseHeadersRO,
        BufferPool bp)
    {
        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = bp.buffer(firstResponseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public boolean storeResponseHeaders(
        ArrayFW<HttpHeaderFW> responseHeaders)
    {
        evictResponseIfNecessary();
        varyBy = getHeader(responseHeaders, HttpHeaders.VARY);
        etag = getHeader(responseHeaders, ETAG);
        resetCacheTiming();

        final int slotCapacity = responsePool.slotCapacity();
        if (slotCapacity < responseHeaders.sizeof())
        {
            return false;
        }

        int headerSlot = responsePool.acquire(requestHash);
        if (headerSlot == NO_SLOT)
        {
            cache.purgeEntriesForNonPendingRequests();
            headerSlot = responsePool.acquire(requestHash);
            if (headerSlot == NO_SLOT)
            {
                return false;
            }
        }
        responseSlots.add(headerSlot);

        MutableDirectBuffer buffer = responsePool.buffer(headerSlot);
        buffer.putBytes(0, responseHeaders.buffer(), responseHeaders.offset(), responseHeaders.sizeof());
        responseHeadersSize = responseHeaders.sizeof();

        return true;
    }

    public void updateResponseHeader(
        String status,
        ArrayFW<HttpHeaderFW> newHeaders)
    {
        final ArrayFW<HttpHeaderFW> responseHeadersSO = new HttpBeginExFW().headers();
        ArrayFW<HttpHeaderFW> oldHeaders = getResponseHeaders(responseHeadersSO);
        String statusCode = Objects.requireNonNull(oldHeaders.matchFirst(h -> Objects.requireNonNull(h.name().asString())
                                                   .toLowerCase().equals(":status"))).value().asString();
        resetCacheTiming();

        final LinkedHashMap<String, String> newHeadersMap = new LinkedHashMap<>();
        oldHeaders.forEach(h -> newHeadersMap.put(h.name().asString(), h.value().asString()));
        newHeaders.forEach(h -> newHeadersMap.put(h.name().asString(), h.value().asString()));
        newHeadersMap.put(":status", statusCode);

        if (NOT_MODIFIED_304.equals(status) &&
            !newHeaders.anyMatch(h -> "date".equalsIgnoreCase(h.name().asString())))
        {
            try
            {
                newHeadersMap.put("date", DATE_FORMAT.format(Date.from(Instant.now())));
            }
            catch (Exception e)
            {
                //NOOP
            }
        }

        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = responsePool.buffer(firstResponseSlot);

        final ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headersRW =
            new ArrayFW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());

        responseHeadersSize = responseBuffer.capacity();
        headersRW.wrap(responseBuffer, 0, responseHeadersSize);

        for (Map.Entry<String, String> entry : newHeadersMap.entrySet())
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

    public void purge()
    {
        evictRequestIfNecessary();
        evictResponseIfNecessary();
    }

    public boolean  canServeRequest(
        ArrayFW<HttpHeaderFW> requestHeaders,
        short authScope)
    {
        Instant now = Instant.now();

        final boolean canBeServedToAuthorized = canBeServedToAuthorized(requestHeaders, authScope);
        final boolean doesNotVaryBy = doesNotVaryBy(requestHeaders);
        final boolean satisfiesFreshnessRequirements = satisfiesFreshnessRequirementsOf(requestHeaders, now);
        final boolean satisfiesStalenessRequirements = satisfiesStalenessRequirementsOf(requestHeaders, now);
        final boolean satisfiesAgeRequirements = satisfiesAgeRequirementsOf(requestHeaders, now);

        return canBeServedToAuthorized &&
               doesNotVaryBy &&
               satisfiesFreshnessRequirements &&
               satisfiesStalenessRequirements &&
               satisfiesAgeRequirements &&
               !validationRequired;
    }

    public void evictRequestIfNecessary()
    {
        if (requestSlot != NO_SLOT)
        {
            requestPool.release(requestSlot);
            requestSlot = NO_SLOT;
        }
    }

    public void evictResponseIfNecessary()
    {
        if (!responseSlots.isEmpty())
        {
            responseSlots.forEach(responsePool::release);
            responseSlots.clear();
            responseSize = 0;
            setResponseCompleted(false);
        }
    }

    private boolean storeResponseData(
        Flyweight data)
    {
        return storeResponseData(data, 0);
    }

    private boolean storeResponseData(
        Flyweight data,
        int written)
    {
        if (data.sizeof() - written == 0)
        {
            return true;
        }

        final int slotCapacity = responsePool.slotCapacity();
        int slotSpaceRemaining = (slotCapacity * (responseSlots.size() - NUM_OF_HEADER_SLOTS)) - responseSize;
        if (slotSpaceRemaining == 0)
        {
            slotSpaceRemaining = slotCapacity;
            int newSlot = responsePool.acquire(requestHash);
            if (newSlot == NO_SLOT)
            {
                cache.purgeEntriesForNonPendingRequests();
                newSlot = responsePool.acquire(requestHash);
                if (newSlot == NO_SLOT)
                {
                    return false;
                }
            }
            responseSlots.add(newSlot);
        }

        int toWrite = Math.min(slotSpaceRemaining, data.sizeof() - written);

        int slot = responseSlots.get(responseSlots.size() - NUM_OF_HEADER_SLOTS);

        MutableDirectBuffer buffer = responsePool.buffer(slot);
        buffer.putBytes(slotCapacity - slotSpaceRemaining, data.buffer(), data.offset() + written, toWrite);
        written += toWrite;
        responseSize += toWrite;
        return storeResponseData(data, written);
    }

    public boolean isStale(
        Instant now)
    {
        final Instant staleAt = staleAt();
        return now.getEpochSecond() > staleAt.getEpochSecond();
    }

    private CacheControl responseCacheControl()
    {
        ArrayFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        String cacheControl = getHeader(responseHeaders, CACHE_CONTROL);
        return cache.responseCacheControl.parse(cacheControl);
    }

    public boolean doesNotVaryBy(
        ArrayFW<HttpHeaderFW> request)
    {
        final ArrayFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
        final ArrayFW<HttpHeaderFW> cachedRequest = getRequestHeaders(cache.requestHeadersRO);

        return CacheUtils.doesNotVary(request, responseHeaders, cachedRequest);
    }

    private boolean canBeServedToAuthorized(
        ArrayFW<HttpHeaderFW> request,
        short requestAuthScope)
    {
        if (SurrogateControl.isProtectedEx(getCachedResponseHeaders()))
        {
            return requestAuthScope == authScope;
        }

        final CacheControl responseCacheControl = responseCacheControl();
        final ArrayFW<HttpHeaderFW> cachedRequestHeaders = getRequestHeaders(cache.requestHeadersRO);
        return sameAuthorizationScope(request, cachedRequestHeaders, responseCacheControl);
    }

    private ArrayFW<HttpHeaderFW> getRequestHeaders(
        ArrayFW<HttpHeaderFW> requestHeaders,
        BufferPool bp)
    {
        final MutableDirectBuffer buffer = bp.buffer(requestSlot);
        return requestHeaders.wrap(buffer, 0, buffer.capacity());
    }

    private boolean satisfiesFreshnessRequirementsOf(
        ArrayFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControl.parse(requestCacheControlHeaderValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MIN_FRESH))
        {
            final String minFresh = requestCacheControl.getValue(MIN_FRESH);
            return now.plusSeconds(parseInt(minFresh)).isBefore(staleAt);
        }
        return true;
    }

    private boolean satisfiesStalenessRequirementsOf(
        ArrayFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControl.parse(requestCacheControlHeaderValue);

        Instant staleAt = staleAt();
        if (requestCacheControl.contains(MAX_STALE))
        {
            final String maxStale = requestCacheControl.getValue(MAX_STALE);
            final int maxStaleSec = (maxStale != null) ? parseInt(maxStale) : MAX_VALUE;
            final Instant acceptable = staleAt.plusSeconds(maxStaleSec);
            return !now.isAfter(acceptable);
        }
        else if (now.isAfter(staleAt))
        {
            return false;
        }

        return true;
    }

    private boolean satisfiesAgeRequirementsOf(
        ArrayFW<HttpHeaderFW> request,
        Instant now)
    {
        final String requestCacheControlHeaderValue = getHeader(request, CACHE_CONTROL);
        final CacheControl requestCacheControl = cache.cachedRequestCacheControl.parse(requestCacheControlHeaderValue);

        if (requestCacheControl.contains(MAX_AGE))
        {
            int requestMaxAge = parseInt(requestCacheControl.getValue(MAX_AGE));
            Instant receivedAt = responseReceivedAt();
            return !receivedAt.plusSeconds(requestMaxAge).isBefore(now);
        }
        return true;
    }

    private Instant staleAt()
    {
        if (cacheStaleAt == null)
        {
            final CacheControl cacheControl = responseCacheControl();
            final Instant receivedAt = responseReceivedAt();
            int staleInSeconds = 0;

            final String sMaxAge = cacheControl.getValue(S_MAXAGE);
            if (sMaxAge != null)
            {
                staleInSeconds = parseInt(sMaxAge);
            }
            else
            {
                final String maxAge = cacheControl.getValue(MAX_AGE);
                if (maxAge != null)
                {
                    staleInSeconds = parseInt(maxAge);
                }
            }

            final int surrogateAge = getSurrogateAge(this.getCachedResponseHeaders());
            staleInSeconds = Math.max(staleInSeconds, surrogateAge);

            cacheStaleAt = receivedAt.plusSeconds(staleInSeconds);
        }

        return cacheStaleAt;
    }

    private Instant responseReceivedAt()
    {
        if (cacheReceivedAt == null)
        {
            final ArrayFW<HttpHeaderFW> responseHeaders = getCachedResponseHeaders();
            final String dateHeaderValue = getHeader(responseHeaders, HttpHeaders.DATE) != null ?
                getHeader(responseHeaders, HttpHeaders.DATE) : getHeader(responseHeaders, HttpHeaders.LAST_MODIFIED);
            try
            {
                if (dateHeaderValue != null)
                {
                    cacheReceivedAt = DATE_FORMAT.parse(dateHeaderValue).toInstant();
                }
                else
                {
                    cacheReceivedAt = Instant.EPOCH;
                }
            }
            catch (Exception e)
            {
                cacheReceivedAt = Instant.EPOCH;
            }
        }

        return cacheReceivedAt;
    }

    private void resetCacheTiming()
    {
        cacheStaleAt = null;
        cacheReceivedAt = null;
    }

}
