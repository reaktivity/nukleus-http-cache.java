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

import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheConfiguration;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DefaultCache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.emulated.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HeapBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public class ProxyStreamFactoryBuilder implements StreamFactoryBuilder
{

    private final HttpCacheConfiguration config;
    private final LongObjectBiConsumer<Runnable> scheduler;
    private final Long2ObjectHashMap<Request> requestCorrelations;
    private final Long2ObjectHashMap<ProxyConnectReplyStream> correlations;
    private final Long2ObjectHashMap<Future<?>> expiryRequestsCorrelations;

    private RouteManager router;
    private MutableDirectBuffer writeBuffer;
    private LongUnaryOperator supplyInitialId;
    private LongSupplier supplyTrace;
    private ToIntFunction<String> supplyTypeId;
    private LongUnaryOperator supplyReplyId;
    private Slab cacheBufferPool;
    private HeapBufferPool requestBufferPool;
    private Cache emulatedCache;
    private DefaultCache defaultCache;
    private BudgetManager emulatedBudgetManager;
    private BudgetManager defaultBudgetManager;
    private Function<String, LongSupplier> supplyCounter;
    private Function<String, LongConsumer> supplyAccumulator;
    private SignalingExecutor executor;

    private LongConsumer cacheEntries;

    public ProxyStreamFactoryBuilder(
            HttpCacheConfiguration config,
            LongObjectBiConsumer<Runnable> scheduler)
    {
        this.config = config;
        this.requestCorrelations = new Long2ObjectHashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.expiryRequestsCorrelations = new Long2ObjectHashMap<>();
        this.scheduler = scheduler;
    }

    @Override
    public ProxyStreamFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyInitialId)
    {
        this.supplyInitialId = supplyInitialId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTraceSupplier(
        LongSupplier supplyTrace)
    {
        this.supplyTrace = supplyTrace;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setGroupBudgetClaimer(
        LongFunction<IntUnaryOperator> groupBudgetClaimer)
    {
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setGroupBudgetReleaser(
        LongFunction<IntUnaryOperator> groupBudgetReleaser)
    {
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        return this;
    }

    @Override
    public StreamFactoryBuilder setCounterSupplier(
        Function<String, LongSupplier> supplyCounter)
    {
        this.supplyCounter = supplyCounter;
        return this;
    }

    @Override
    public StreamFactoryBuilder setAccumulatorSupplier(
            Function<String, LongConsumer> supplyAccumulator)
    {
        this.supplyAccumulator = supplyAccumulator;
        return this;
    }

    @Override
    public StreamFactoryBuilder setExecutor(
        SignalingExecutor executor)
    {
        this.executor = executor;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        final HttpCacheCounters counters = new HttpCacheCounters(supplyCounter, supplyAccumulator);

        if (emulatedBudgetManager == null && defaultBudgetManager == null)
        {
            emulatedBudgetManager = new BudgetManager();
            defaultBudgetManager =  new BudgetManager();
            final int httpCacheCapacity = config.cacheCapacity();
            final int httpCacheSlotCapacity = config.cacheSlotCapacity();
            this.cacheBufferPool = new Slab(httpCacheCapacity, httpCacheSlotCapacity);
            this.requestBufferPool = new HeapBufferPool(config.maximumRequests(), httpCacheSlotCapacity);
        }

        if (cacheEntries == null)
        {
            cacheEntries = supplyAccumulator.apply("http-cache.cache.entries");
        }

        if (emulatedCache == null)
        {
            this.emulatedCache = new Cache(
                    scheduler,
                    emulatedBudgetManager,
                    writeBuffer,
                    requestBufferPool,
                    cacheBufferPool,
                    requestCorrelations,
                    counters,
                    cacheEntries,
                    supplyTrace,
                    supplyTypeId);
        }

        if (defaultCache == null)
        {
            this.defaultCache = new DefaultCache(
                scheduler,
                writeBuffer,
                cacheBufferPool,
                requestCorrelations,
                counters,
                cacheEntries,
                supplyTrace,
                supplyTypeId,
                executor);
        }

        return new ProxyStreamFactory(
                router,
                defaultBudgetManager,
                writeBuffer,
                requestBufferPool,
                supplyInitialId,
                supplyReplyId,
                requestCorrelations,
                correlations,
                expiryRequestsCorrelations,
                emulatedCache,
                defaultCache,
                counters,
                supplyTrace,
                supplyTypeId,
                executor,
                scheduler);
    }

}
