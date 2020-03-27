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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheUtils.RESPONSE_IS_STALE;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.getPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferWait;
import static org.reaktivity.nukleus.http_cache.internal.proxy.cache.PreferHeader.isPreferenceApplied;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.IF_NONE_MATCH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.PREFERENCE_APPLIED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.RETRY_AFTER;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.WARNING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.HAS_EMULATED_PROTOCOL_STACK;
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
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.SurrogateControl;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW;
import org.reaktivity.nukleus.http_cache.internal.types.Array32FW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.String8FW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpEndExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;

public class Writer
{
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    private final HttpEndExFW.Builder httpEndExRW = new HttpEndExFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();
    private final CacheControl cacheControlParser = new CacheControl();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final int httpTypeId;

    public Writer(
        RouteManager router,
        ToIntFunction<String> supplyTypeId,
        MutableDirectBuffer writeBuffer)
    {
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.httpTypeId = supplyTypeId.applyAsInt("http");
    }

    public void doHttpRequest(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(0L)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpResponse(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .affinity(0L)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpResponseWithUpdatedHeaders(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        Array32FW<HttpHeaderFW> responseHeaders,
        Array32FW<HttpHeaderFW> requestHeaders,
        String etag,
        boolean isStale,
        long traceId)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
            builder -> updateResponseHeaders(builder,
                                             responseHeaders,
                                             requestHeaders,
                                             etag,
                                             isStale);

        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .affinity(0L)
                                     .extension(e -> e.set(visitHttpBeginEx(mutator)))
                                     .build();
        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void updateResponseHeaders(
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        Array32FW<HttpHeaderFW> responseHeaders,
        Array32FW<HttpHeaderFW> requestHeaders,
        String etag,
        boolean isStale)
    {
        final int staleWhileRevalidate = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        final boolean hasPreferWait = isPreferWait(requestHeaders);
        final boolean isEmulatedProtocolStack = requestHeaders.anyMatch(HAS_EMULATED_PROTOCOL_STACK);
        responseHeaders.forEach(h ->
        {
            final String8FW name = h.name();
            final String16FW value = h.value();
            if (!RETRY_AFTER.equals(name.asString()) &&
                !CACHE_CONTROL.equals(name.asString()))
            {
                builder.item(header -> header.name(name).value(value));
            }

            updateEmulatedResponseHeaders(builder, h, staleWhileRevalidate, hasPreferWait, isEmulatedProtocolStack);
        });

        if (!responseHeaders.anyMatch(h -> ETAG.equals(h.name().asString())) && etag != null)
        {
            builder.item(header -> header.name(ETAG).value(etag));
        }

        if (isPreferWait(requestHeaders) && !isPreferenceApplied(responseHeaders))
        {
            builder.item(header -> header.name(PREFERENCE_APPLIED)
                                         .value("wait=" + getPreferWait(requestHeaders)));
        }

        if (isPreferWait(requestHeaders))
        {
            builder.item(header -> header.name(ACCESS_CONTROL_EXPOSE_HEADERS)
                                         .value(String.format("%s, %s", PREFERENCE_APPLIED, ETAG)));
        }

        if (!responseHeaders.anyMatch(HAS_CACHE_CONTROL) && isEmulatedProtocolStack)
        {
            final String value = hasPreferWait
                ? "private, stale-while-revalidate=" + staleWhileRevalidate
                : "stale-while-revalidate=" + staleWhileRevalidate;
            builder.item(header -> header.name("cache-control").value(value));
        }

        if (isStale)
        {
            builder.item(header -> header.name(WARNING).value(RESPONSE_IS_STALE));
        }
    }

    private void updateEmulatedResponseHeaders(
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        HttpHeaderFW responseHeader,
        int staleWhileRevalidate,
        boolean hasPreferWait,
        boolean isEmulatedProtocolStack)
    {
        final String8FW nameFW = responseHeader.name();
        final String name = nameFW.asString();
        final String16FW valueFW = responseHeader.value();
        final String value = valueFW.asString();

        if (HttpHeaders.CACHE_CONTROL.equals(name))
        {
            if (isEmulatedProtocolStack)
            {
                cacheControlParser.parse(value);
                cacheControlParser.getValues().put("stale-while-revalidate", "" + staleWhileRevalidate);
                if (hasPreferWait && !(cacheControlParser.contains("private") || cacheControlParser.contains("public")))
                {
                    cacheControlParser.getValues().put("private", null);
                }
                StringBuilder cacheControlDirectives = new StringBuilder();
                cacheControlParser.getValues().forEach((k, v) ->
                {
                    cacheControlDirectives.append(cacheControlDirectives.length() > 0 ? ", " : "");
                    cacheControlDirectives.append(k);
                    if (v != null)
                    {
                        cacheControlDirectives.append('=').append(v);
                    }
                });
                builder.item(header -> header.name(nameFW).value(cacheControlDirectives.toString()));
            }
            else
            {
                builder.item(header -> header.name(nameFW).value(valueFW));
            }
        }
    }

    public void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long budgetId,
        DirectBuffer payload,
        int offset,
        int length,
        int reserved)
    {

        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(p -> p.set(payload, offset, length))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long budgetId,
        int reserved,
        Consumer<OctetsFW.Builder> payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpEnd(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId,
        String etag)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator =
            builder -> updateTrailer(builder, etag);

        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
                               .traceId(traceId)
                               .extension(e -> e.set(visitHttpEndEx(mutator)))
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
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
                .traceId(traceId)
                .extension(extension)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void updateTrailer(
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        String etag)
    {
        builder.item(header -> header.name(ETAG).value(etag));
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
                .traceId(traceId)
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
                .traceId(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    public void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final long budgetId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
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
                .traceId(traceId)
                .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
        Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .typeId(httpTypeId)
                             .headers(headers)
                             .build()
                             .sizeof();
    }

    private Flyweight.Builder.Visitor visitHttpEndEx(
        Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> trailers)
    {
        return (buffer, offset, limit) ->
            httpEndExRW.wrap(buffer, offset, limit)
                         .typeId(httpTypeId)
                         .trailers(trailers)
                         .build()
                         .sizeof();
    }

    public void doHttpPushPromise(
        MessageConsumer acceptReply,
        long routeId,
        long streamId,
        long authorization,
        Array32FW<HttpHeaderFW>  requestHeaders,
        Array32FW<HttpHeaderFW> responseHeaders,
        String etag)
    {
        final int staleWhileRevalidate = SurrogateControl.getSurrogateFreshnessExtension(responseHeaders);
        doH2PushPromise(acceptReply,
                        routeId,
                        streamId,
                        authorization,
                        0L,
                        0,
                        setPushPromiseHeaders(requestHeaders, responseHeaders, staleWhileRevalidate, etag));
    }

    private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> setPushPromiseHeaders(
        Array32FW<HttpHeaderFW> requestHeaders,
        Array32FW<HttpHeaderFW> responseHeaders,
        int freshnessExtension,
        String etag)
    {
        Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> result =
            builder -> updateRequestHeaders(requestHeaders, responseHeaders, builder, freshnessExtension, etag);

        return result;
    }

    private void updateRequestHeaders(
        Array32FW<HttpHeaderFW> requestHeaders,
        Array32FW<HttpHeaderFW> responseHeaders,
        Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
        int freshnessExtension,
        String etag)
    {
        requestHeaders.forEach(h ->
        {
            final String8FW nameFW = h.name();
            final String name = nameFW.asString();
            final String16FW valueFW = h.value();
            final String value = valueFW.asString();

            switch (name)
            {
            case HttpHeaders.METHOD:
            case HttpHeaders.AUTHORITY:
            case HttpHeaders.SCHEME:
            case HttpHeaders.PATH:
            case HttpHeaders.PREFER:
                builder.item(header -> header.name(nameFW).value(valueFW));
                break;
            case HttpHeaders.CACHE_CONTROL:
                if (value.contains(CacheDirectives.MAX_AGE_0))
                {
                    builder.item(header -> header.name(nameFW)
                                                 .value(valueFW));
                }
                else
                {
                    builder.item(header -> header.name(nameFW)
                                                 .value(value + ", max-age=0"));
                }
                break;
            case HttpHeaders.IF_MODIFIED_SINCE:
                if (responseHeaders.anyMatch(h2 -> "last-modified".equals(h2.name().asString())))
                {
                    final String newValue = getHeader(responseHeaders, "last-modified");
                    builder.item(header -> header.name(nameFW)
                                                 .value(newValue));
                }
                break;
            case HttpHeaders.IF_NONE_MATCH:
                String result = etag;
                if (responseHeaders.anyMatch(h2 -> "etag".equals(h2.name().asString())))
                {
                    final String existingIfNoneMatch = getHeader(responseHeaders, "etag");
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
                if (CacheUtils.isVaryHeader(name, responseHeaders))
                {
                    builder.item(header -> header.name(nameFW).value(valueFW));
                }
            }
        });
        if (!requestHeaders.anyMatch(HAS_CACHE_CONTROL))
        {
            builder.item(header -> header.name("cache-control").value("max-age=0"));
        }
        if (!requestHeaders.anyMatch(PreferHeader.PREFER_HEADER_NAME))
        {
            builder.item(header -> header.name("prefer").value("wait=" + freshnessExtension));
        }
        if (!requestHeaders.anyMatch(h -> HttpHeaders.IF_NONE_MATCH.equals(h.name().asString())))
        {
            builder.item(header -> header.name(IF_NONE_MATCH).value(etag));
        }
    }

    private void doH2PushPromise(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long authorization,
        long budgetId,
        int reserved,
        Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload((OctetsFW) null)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }
}
