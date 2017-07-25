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
package org.reaktivity.nukleus.http_cache.internal.stream;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public class ProxyStreamFactoryBuilder implements StreamFactoryBuilder
{

   private final Long2ObjectHashMap<Correlation> correlations;

    private RouteHandler router;
    private MutableDirectBuffer writeBuffer;
    private LongSupplier supplyStreamId;
    private LongSupplier supplyCorrelationId;
    private Supplier<BufferPool> supplyBufferPool;

    private LongObjectBiConsumer<Runnable> scheduler;

    public ProxyStreamFactoryBuilder(LongObjectBiConsumer<Runnable> scheduler)
    {
        this.correlations = new Long2ObjectHashMap<>();
        this.scheduler = scheduler;
    }

    @Override
    public ProxyStreamFactoryBuilder setRouteHandler(
        RouteHandler router)
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
    public ProxyStreamFactoryBuilder setStreamIdSupplier(
        LongSupplier supplyStreamId)
    {
        this.supplyStreamId = supplyStreamId;
        return this;
    }

    @Override
    public ProxyStreamFactoryBuilder setCorrelationIdSupplier(
        LongSupplier supplyCorrelationId)
    {
        this.supplyCorrelationId = supplyCorrelationId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        final BufferPool bufferPool = supplyBufferPool.get();
        final Cache cache = new Cache(
                writeBuffer,
                supplyStreamId,
                supplyCorrelationId,
                bufferPool);

        return new ProxyStreamFactory(
                router,
                writeBuffer,
                bufferPool,
                supplyStreamId,
                supplyCorrelationId,
                correlations,
                scheduler,
                cache);
    }

}
