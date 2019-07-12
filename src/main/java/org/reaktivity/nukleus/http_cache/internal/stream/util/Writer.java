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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;

public class Writer
{
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();

    private final MutableDirectBuffer writeBuffer;
    private final int httpTypeId;

    public Writer(
        ToIntFunction<String> supplyTypeId,
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        this.httpTypeId = supplyTypeId.applyAsInt("http");
    }

    public void doHttpRequest(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        Consumer<Builder<HttpHeaderFW.Builder,
        HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpResponse(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        Consumer<Builder<HttpHeaderFW.Builder,
        HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpResponseWithUpdatedCacheControl(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        CacheControl cacheControlFW,
        ListFW<HttpHeaderFW> responseHeaders,
        int staleWhileRevalidate,
        String etag,
        boolean cacheControlPrivate,
        long traceId)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
                builder -> updateResponseHeaders(builder, cacheControlFW, responseHeaders, staleWhileRevalidate,
                        etag, cacheControlPrivate);
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();
        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void updateResponseHeaders(
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        CacheControl cacheControlFW,
        ListFW<HttpHeaderFW> responseHeadersRO,
        int staleWhileRevalidate,
        String etag,
        boolean cacheControlPrivate)
    {
        responseHeadersRO.forEach(h ->
        {
            final StringFW nameFW = h.name();
            final String name = nameFW.asString();
            final String16FW valueFW = h.value();
            final String value = valueFW.asString();

            switch(name)
            {
                case HttpHeaders.CACHE_CONTROL:
                    cacheControlFW.parse(value);
                    cacheControlFW.getValues().put("stale-while-revalidate", "" + staleWhileRevalidate);
                    if (cacheControlPrivate && !(cacheControlFW.contains("private") || cacheControlFW.contains("public")))
                    {
                        cacheControlFW.getValues().put("private", null);
                    }
                    StringBuilder cacheControlDirectives = new StringBuilder();
                    cacheControlFW.getValues().forEach((k, v) ->
                    {
                        cacheControlDirectives.append(cacheControlDirectives.length() > 0 ? ", " : "");
                        cacheControlDirectives.append(k);
                        if (v != null)
                        {
                            cacheControlDirectives.append('=').append(v);
                        }
                    });
                    builder.item(header -> header.name(nameFW).value(cacheControlDirectives.toString()));
                    break;
                default: builder.item(header -> header.name(nameFW).value(valueFW));
            }
        });
        if (!responseHeadersRO.anyMatch(HAS_CACHE_CONTROL))
        {
            final String value = cacheControlPrivate
                    ? "private, stale-while-revalidate=" + staleWhileRevalidate
                    : "stale-while-revalidate=" + staleWhileRevalidate;
            builder.item(header -> header.name("cache-control").value(value));
        }
        if (!responseHeadersRO.anyMatch(h -> ETAG.equals(h.name().asString())) && etag != null)
        {
            builder.item(header -> header.name(ETAG).value(etag));
        }
    }

    public void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long groupId,
        DirectBuffer payload,
        int offset,
        int length,
        int padding)
    {

        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .groupId(groupId)
                .padding(padding)
                .payload(p -> p.set(payload, offset, length))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long groupId,
        int padding,
        Consumer<OctetsFW.Builder> payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .groupId(groupId)
                .padding(padding)
                .payload(payload)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpEnd(
            final MessageConsumer receiver,
            final long routeId,
            final long streamId,
            final long traceId,
            OctetsFW extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .extension(extension)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public void doHttpEnd(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public void doAbort(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    public void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    public void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .typeId(httpTypeId)
                             .headers(headers)
                             .build()
                             .sizeof();
    }

    public void doHttpPushPromise(
        AnswerableByCacheRequest request,
        CacheableRequest cachedRequest,
        ListFW<HttpHeaderFW> responseHeaders,
        int freshnessExtension,
        String etag)
    {
        final ListFW<HttpHeaderFW> requestHeaders = cachedRequest.getRequestHeaders(requestHeadersRO);
        final MessageConsumer acceptReply = request.acceptReply();
        final long routeId = request.acceptRouteId();
        final long streamId = request.acceptReplyId();
        final long authorization = request.authorization();

        doH2PushPromise(
            acceptReply,
            routeId, streamId, authorization, 0L, 0,
            setPushPromiseHeaders(requestHeaders, responseHeaders, freshnessExtension, etag));
    }

    private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setPushPromiseHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO,
        ListFW<HttpHeaderFW> responseHeadersRO,
        int freshnessExtension,
        String etag)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> result =
                builder -> updateRequestHeaders(requestHeadersRO, responseHeadersRO, builder, freshnessExtension, etag);

        return result;
    }

    private void updateRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersFW,
        ListFW<HttpHeaderFW> responseHeadersFW,
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        int freshnessExtension,
        String etag)
    {
        requestHeadersFW
           .forEach(h ->
           {
               final StringFW nameFW = h.name();
               final String name = nameFW.asString();
               final String16FW valueFW = h.value();
               final String value = valueFW.asString();

               switch(name)
               {
                   case HttpHeaders.METHOD:
                   case HttpHeaders.AUTHORITY:
                   case HttpHeaders.SCHEME:
                   case HttpHeaders.PATH:
                       builder.item(header -> header.name(nameFW).value(valueFW));
                       break;
                   case HttpHeaders.CACHE_CONTROL:
                       if (value.contains(CacheDirectives.NO_CACHE))
                       {
                           builder.item(header -> header.name(nameFW)
                                                        .value(valueFW));
                       }
                       else
                       {
                           builder.item(header -> header.name(nameFW)
                                                        .value(value + ", no-cache"));
                       }
                       break;
                   case HttpHeaders.IF_MODIFIED_SINCE:
                       if (responseHeadersFW.anyMatch(h2 -> "last-modified".equals(h2.name().asString())))
                       {
                           final String newValue = getHeader(responseHeadersFW, "last-modified");
                           builder.item(header -> header.name(nameFW)
                                                        .value(newValue));
                       }
                       break;
                   case HttpHeaders.IF_NONE_MATCH:
                       String result = etag;
                       if (responseHeadersFW.anyMatch(h2 -> "etag".equals(h2.name().asString())))
                       {
                           final String existingIfNoneMatch = getHeader(responseHeadersFW, "etag");
                           if (!existingIfNoneMatch.contains(etag))
                           {
                               result += ", " + existingIfNoneMatch;
                           }
                           else
                           {
                               result = existingIfNoneMatch;
                           }
                       }
                       final String finalEtag = result;
                       builder.item(header -> header.name(nameFW)
                               .value(finalEtag));
                       break;
                   case HttpHeaders.IF_MATCH:
                   case HttpHeaders.IF_UNMODIFIED_SINCE:
                        break;
                   default:
                       if (CacheUtils.isVaryHeader(name, responseHeadersFW))
                       {
                           builder.item(header -> header.name(nameFW).value(valueFW));
                       }
               }
           });
           if (!requestHeadersFW.anyMatch(HAS_CACHE_CONTROL))
           {
               builder.item(header -> header.name("cache-control").value("no-cache"));
           }
           if (!requestHeadersFW.anyMatch(PreferHeader.PREFER_HEADER_NAME))
           {
               builder.item(header -> header.name("prefer").value("wait=" + freshnessExtension));
           }
           if (!requestHeadersFW.anyMatch(h -> HttpHeaders.IF_NONE_MATCH.equals(h.name().asString())))
           {
               builder.item(header -> header.name(IF_NONE_MATCH).value(etag));
           }
    }

    private void doH2PushPromise(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long authorization,
        long groupId,
        int padding,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .authorization(authorization)
                .groupId(groupId)
                .padding(padding)
                .payload((OctetsFW) null)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void do503AndAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long correlationId)
    {
        this.doHttpResponse(receiver, routeId, streamId, traceId, e -> e.item(h -> h.name(STATUS).value("503")));
        this.doAbort(receiver, routeId, streamId, traceId);
    }

}
