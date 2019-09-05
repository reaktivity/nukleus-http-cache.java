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
package org.reaktivity.nukleus.http_cache.internal.proxy.request.emulated;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class Request
{
    public enum Type
    {
        PREFER_WAIT, PROXY, INITIAL_REQUEST, CACHE_REFRESH, DEFAULT_REQUEST
    }

    public final MessageConsumer acceptReply;
    public final long acceptRouteId;
    public final long acceptReplyId;
    public final RouteManager router;

    private final boolean isEmulated;

    public Request(
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptReplyId,
        RouteManager router,
        boolean isEmulated)
    {
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptReplyId = acceptReplyId;
        this.router = router;
        this.isEmulated = isEmulated;
    }

    public abstract Type getType();

    public MessageConsumer acceptReply()
    {
        return acceptReply;
    }

    public long acceptRouteId()
    {
        return acceptRouteId;
    }

    public long acceptReplyId()
    {
        return acceptReplyId;
    }

    public void setThrottle(MessageConsumer throttle)
    {
        this.router.setThrottle(acceptReplyId, throttle);
    }

    public abstract void purge();

    public boolean isEmulated()
    {
        return isEmulated;
    }
}
