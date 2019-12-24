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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.FrameFW;
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

    HttpCacheProxyRelayedResponse(
        HttpCacheProxyFactory factory,
        MessageConsumer receiver,
        long receiverRouteId,
        long receiverReplyId,
        MessageConsumer sender,
        long senderRouteId,
        long senderReplyId)
    {
        this.factory = factory;
        this.receiver = receiver;
        this.receiverRouteId = receiverRouteId;
        this.receiverReplyId = receiverReplyId;
        this.sender = sender;
        this.senderRouteId = senderRouteId;
        this.senderReplyId = senderReplyId;
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

        writeBuffer.putBytes(0, buffer, index, index + length);
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            factory.router.setThrottle(receiverReplyId, this::onResponseMessage);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, receiverRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, receiverReplyId);
            receiver.accept(msgTypeId, writeBuffer, 0, length);
            break;
        case DataFW.TYPE_ID:
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, receiverRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, receiverReplyId);
            receiver.accept(msgTypeId, writeBuffer, 0, length);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, receiverRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, receiverReplyId);
            receiver.accept(msgTypeId, writeBuffer, 0, length);
            factory.router.clearThrottle(receiverReplyId);
            break;
        case ResetFW.TYPE_ID:
        case WindowFW.TYPE_ID:
        case SignalFW.TYPE_ID:
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_ROUTE_ID, senderRouteId);
            writeBuffer.putLong(FrameFW.FIELD_OFFSET_STREAM_ID, senderReplyId);
            sender.accept(msgTypeId, writeBuffer, 0, length);
            break;
        default:
            break;
        }
    }
}
