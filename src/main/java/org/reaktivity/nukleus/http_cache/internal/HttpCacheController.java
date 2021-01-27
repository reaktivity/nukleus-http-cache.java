/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.http_cache.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static org.reaktivity.nukleus.route.RouteKind.PROXY;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.concurrent.CompletableFuture;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.FreezeFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.Role;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.route.RouteKind;

public final class HttpCacheController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: HttpPushConfiguration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
    private final FreezeFW.Builder freezeRW = new FreezeFW.Builder();

    private final OctetsFW extensionRO = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer commandBuffer;

    public HttpCacheController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.commandBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<HttpCacheController> kind()
    {
        return HttpCacheController.class;
    }

    @Override
    public String name()
    {
        return HttpCacheNukleus.NAME;
    }

    @Deprecated
    public CompletableFuture<Long> routeServer(
        String localAddress,
        String remoteAddress)
    {
        return doRoute(SERVER, localAddress, remoteAddress, extensionRO);
    }

    @Deprecated
    public CompletableFuture<Long> routeProxy(
        String localAddress,
        String remoteAddress)
    {
        return doRoute(PROXY, localAddress, remoteAddress, extensionRO);
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress)
    {
        return doRoute(kind, localAddress, remoteAddress, extensionRO);
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                     .correlationId(correlationId)
                                     .nukleus(name())
                                     .routeId(routeId)
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public CompletableFuture<Void> freeze()
    {
        long correlationId = controllerSpi.nextCorrelationId();

        FreezeFW freeze = freezeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                  .correlationId(correlationId)
                                  .nukleus(name())
                                  .build();

        return controllerSpi.doFreeze(freeze.typeId(), freeze.buffer(), freeze.offset(), freeze.sizeof());
    }

    private CompletableFuture<Long> doRoute(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        Flyweight extension)
    {
        final long correlationId = controllerSpi.nextCorrelationId();
        final Role role = Role.valueOf(kind.ordinal());

        final RouteFW route = routeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                .correlationId(correlationId)
                .nukleus(name())
                .role(b -> b.set(role))
                .localAddress(localAddress)
                .remoteAddress(remoteAddress)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }
}
