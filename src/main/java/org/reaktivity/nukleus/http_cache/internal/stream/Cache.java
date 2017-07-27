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

import static org.reaktivity.nukleus.http_cache.util.HttpCacheUtils.responseCanSatisfyRequest;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_cache.util.HttpCacheUtils;

public class Cache
{

    private final Writer writer;
    private final Long2ObjectHashMap<CacheResponseServer> requestURLToResponse;
    private final BufferPool requestBufferPool;
    private final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    private final BufferPool responseBufferPool;
    private final ListFW<HttpHeaderFW> responseHeadersRO = new HttpBeginExFW().headers();
    private final LongSupplier streamSupplier;
    private final LongSupplier supplyCorrelationId;
    private final WindowFW windowRO = new WindowFW();

    public Cache(
            MutableDirectBuffer writeBuffer,
            LongSupplier streamSupplier,
            LongSupplier supplyCorrelationId,
            BufferPool bufferPool)
    {
        this.streamSupplier = streamSupplier;
        this.supplyCorrelationId = supplyCorrelationId;
        this.writer = new Writer(writeBuffer);
        this.requestBufferPool = bufferPool;
        this.responseBufferPool = bufferPool.duplicate();
        this.requestURLToResponse = new Long2ObjectHashMap<>();
    }

    public void put(
        int requestURLHash,
        int requestSlot,
        int requestSize,
        int responseSlot,
        int responseHeaderSize,
        int responseSize)
    {
        CacheResponseServer responseServer = new CacheResponseServer(
                requestURLHash,
                requestSlot,
                requestSize,
                responseSlot,
                responseHeaderSize,
                responseSize);

        CacheResponseServer oldResponseServer = requestURLToResponse.put(requestURLHash, responseServer);
        if (oldResponseServer != null)
        {
            oldResponseServer.cleanUp();
        }
    }

    public CacheResponseServer get(int requestURLHash)
    {
        return requestURLToResponse.get(requestURLHash);
    }

    public class CacheResponseServer
    {

        private final int requestURLHash;
        private final int requestSlot;
        private final int requestSize;
        private final int responseSlot;
        private final int responseHeaderSize;
        private final int responseSize;
        private int clientCount = 0;
        private boolean cleanUp = false;

        public CacheResponseServer(
            int requestURLHash,
            int requestSlot,
            int requestSize,
            int responseSlot,
            int responseHeaderSize,
            int responseSize)
        {
            this.requestURLHash = requestURLHash;
            this.requestSlot = requestSlot;
            this.requestSize = requestSize;
            this.responseSlot = responseSlot;
            this.responseHeaderSize = responseHeaderSize;
            this.responseSize = responseSize;
        }

        public void handleEndOfStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            this.removeClient();
        }

        public void serveClient(
                Correlation streamCorrelation)
        {
            addClient();
            MutableDirectBuffer buffer = requestBufferPool.buffer(responseSlot);
            ListFW<HttpHeaderFW> responseHeaders = responseHeadersRO.wrap(buffer, 0, responseHeaderSize);

            final long correlation = supplyCorrelationId.getAsLong();
            final MessageConsumer messageConsumer = streamCorrelation.consumer();
            final long streamId = streamSupplier.getAsLong();
            streamCorrelation.setConnectReplyThrottle(
                    new ServeFromCacheStream(
                            messageConsumer,
                            streamId,
                            responseSlot,
                            responseHeaderSize,
                            responseSize,
                            this::handleEndOfStream));

            writer.doHttpBegin(messageConsumer, streamId, 0L, correlation,
                    e -> responseHeaders.forEach(h ->
                    e.item(h2 ->
                    {
                        StringFW name = h.name();
                        String16FW value = h.value();
                        h2.representation((byte) 0)
                                        .name(name)
                                        .value(value);
                    })));
        }

        public void cleanUp()
        {
            cleanUp = true;
            if (clientCount == 0)
            {
                responseBufferPool.release(responseSlot);
                requestBufferPool.release(requestSlot);
            }
        }

        class ServeFromCacheStream implements MessageConsumer
        {
            private final  MessageConsumer messageConsumer;
            private final long streamId;

            private int payloadWritten;
            private int responseSlot;
            private int responseHeaderSize;
            private int responseSize;
            private MessageConsumer onEnd;

             ServeFromCacheStream(
                MessageConsumer messageConsumer,
                long streamId,
                int responseSlot,
                int responseHeaderSize,
                int responseSize,
                MessageConsumer onEnd)
            {
                this.payloadWritten = 0;
                this.messageConsumer = messageConsumer;
                this.streamId = streamId;
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
                        final WindowFW window = windowRO.wrap(buffer, index, index + length);
                        int update = window.update();
                        writePayload(update);
                        break;
                    case ResetFW.TYPE_ID:
                        this.onEnd.accept(msgTypeId, buffer, index, length);
                        break;
                }
            }

            private void writePayload(int update)
            {
                final int toWrite = Math.min(update, responseSize - payloadWritten);
                final int offset = responseHeaderSize + payloadWritten;
                MutableDirectBuffer buffer = responseBufferPool.buffer(responseSlot);
                writer.doHttpData(messageConsumer, streamId, buffer, offset, toWrite);
                payloadWritten += toWrite;
                if (payloadWritten == responseSize)
                {
                    writer.doHttpEnd(messageConsumer, streamId);

                    // TODO these values don't make sense but aren't used
                    this.onEnd.accept(EndFW.TYPE_ID, buffer, offset, toWrite);
                }
            }
        }

        public ListFW<HttpHeaderFW> getRequest()
        {
            DirectBuffer buffer = requestBufferPool.buffer(requestSlot);
            return requestHeadersRO.wrap(buffer, 0, requestSize);
        }

        public ListFW<HttpHeaderFW> getResponseHeaders()
        {
            DirectBuffer buffer = responseBufferPool.buffer(responseSlot);
            return responseHeadersRO.wrap(buffer, 0, responseHeaderSize);
        }

        public OctetsFW getResponse(OctetsFW octetsFW)
        {
            DirectBuffer buffer = responseBufferPool.buffer(responseSlot);
            return octetsFW.wrap(buffer, responseHeaderSize, responseSize);
        }

        public void addClient()
        {
            this.clientCount++;
        }

        public void removeClient()
        {
            clientCount--;
            if (clientCount == 0 && cleanUp)
            {
                responseBufferPool.release(responseSlot);
                requestBufferPool.release(requestSlot);
            }
        }
    }

    public CacheResponseServer hasStoredResponseThatSatisfies(
            int requestURLHash,
            ListFW<HttpHeaderFW> myRequestHeaders,
            boolean isRevalidating)
    {
        CacheResponseServer responseServer = requestURLToResponse.get(requestURLHash);
        if (responseServer == null)
        {
            return null;
        }

        ListFW<HttpHeaderFW> cacheRequestHeaders = responseServer.getRequest();
        ListFW<HttpHeaderFW> responseHeaders = responseServer.getResponseHeaders();
        if (!isRevalidating && HttpCacheUtils.isExpired(responseHeaders))
        {
            responseServer.cleanUp();
            requestURLToResponse.remove(requestURLHash);
            return null;
        }
        if (responseCanSatisfyRequest(cacheRequestHeaders, myRequestHeaders, responseHeaders))
        {
            return responseServer;
        }
        else
        {
            return null;
        }
    }

}
