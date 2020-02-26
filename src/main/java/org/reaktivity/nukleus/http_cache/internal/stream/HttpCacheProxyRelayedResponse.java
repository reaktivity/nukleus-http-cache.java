/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.types.ArrayFW;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public final class HttpCacheProxyRelayedResponse
{
    private final HttpCacheProxyFactory factory;
    private final MessageConsumer receiver;
    private final long receiverRouteId;
    private final long receiverReplyId;
    private final MessageConsumer sender;
    private final long senderRouteId;
    private final long senderReplyId;
    private final String prefer;

    HttpCacheProxyRelayedResponse(
        HttpCacheProxyFactory factory,
        MessageConsumer receiver,
        long receiverRouteId,
        long receiverReplyId,
        MessageConsumer sender,
        long senderRouteId,
        long senderReplyId,
        String prefer)
    {
        this.factory = factory;
        this.receiver = receiver;
        this.receiverRouteId = receiverRouteId;
        this.receiverReplyId = receiverReplyId;
        this.sender = sender;
        this.senderRouteId = senderRouteId;
        this.senderReplyId = senderReplyId;
        this.prefer = prefer;
    }

    void doResponseReset(
        long traceId)
    {
        factory.writer.doReset(receiver, receiverRouteId, senderReplyId, traceId);
    }

    void onResponseMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final MutableDirectBuffer writeBuffer = factory.writeBuffer;

        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);
            onResponseBegin(begin);
            break;
        case DataFW.TYPE_ID:
            writeBuffer.putBytes(0, buffer, index, length);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, receiverRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, receiverReplyId);
            receiver.accept(msgTypeId, writeBuffer, 0, length);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            writeBuffer.putBytes(0, buffer, index, length);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, receiverRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, receiverReplyId);
            receiver.accept(msgTypeId, writeBuffer, 0, length);
            factory.router.clearThrottle(receiverReplyId);
            break;
        case ResetFW.TYPE_ID:
        case WindowFW.TYPE_ID:
        case SignalFW.TYPE_ID:
            writeBuffer.putBytes(0, buffer, index, length);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, senderRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, senderReplyId);
            sender.accept(msgTypeId, writeBuffer, 0, length);
            break;
        default:
            break;
        }
    }

    private void onResponseBegin(
        BeginFW begin)
    {
        final long traceId = begin.traceId();
        final long authorization = begin.authorization();
        final long affinity = begin.affinity();
        final OctetsFW extension = begin.extension();


        final HttpBeginExFW httpBeginEx = extension.get(factory.httpBeginExRO::tryWrap);
        final HttpBeginExFW httpBeginEx0 = (httpBeginEx == null) ? factory.defaultHttpBeginExRO : httpBeginEx;

        final boolean hasEtag = httpBeginEx0.headers().matchFirst(h -> ETAG.equals(h.name().asString())) != null;
        final HttpHeaderFW status = httpBeginEx0.headers().matchFirst(h -> STATUS.equals(h.name().asString()));
        final int statusGroup = status != null ? parseInt(status.value().asString()) / 100 : 0;
        final Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = hs ->
        {
            httpBeginEx0.headers().forEach(h -> hs.item(i -> i.name(h.name()).value(h.value())));
            if ((statusGroup == 2 || statusGroup == 3) && prefer != null)
            {
                hs.item(h -> h.name(PREFERENCE_APPLIED).value(prefer));
                final String exposedHeaders = hasEtag ? String.format("%s, %s", PREFERENCE_APPLIED, ETAG) : PREFERENCE_APPLIED;
                hs.item(h -> h.name(ACCESS_CONTROL_EXPOSE_HEADERS).value(exposedHeaders));
            }
        };

        Flyweight.Builder.Visitor mutator = (b, o, l) ->
            factory.httpBeginExRW.wrap(b, o, l)
                                 .typeId(httpBeginEx0.typeId())
                                 .headers(headers)
                                 .build()
                                 .sizeof();

        final MutableDirectBuffer writeBuffer = factory.writeBuffer;
        final BeginFW newBegin = factory.beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                                .routeId(receiverRouteId)
                                                .streamId(receiverReplyId)
                                                .traceId(traceId)
                                                .authorization(authorization)
                                                .affinity(affinity)
                                                .extension(e -> e.set(mutator))
                                                .build();

        factory.router.setThrottle(receiverReplyId, this::onResponseMessage);
        receiver.accept(newBegin.typeId(), newBegin.buffer(), newBegin.offset(), newBegin.sizeof());

    }
}
