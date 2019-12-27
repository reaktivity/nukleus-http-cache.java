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

final class HttpCacheRequestState
{
    private static final int INITIAL_OPENING = 0x01;
    private static final int INITIAL_OPENED = 0x02;
    private static final int INITIAL_CLOSED = 0x04;
    private static final int REPLY_OPENED = 0x10;
    private static final int REPLY_CLOSING = 0x20;
    private static final int REPLY_CLOSED = 0x40;

    static int openingInitial(
        int state)
    {
        return state | INITIAL_OPENING;
    }

    static int openInitial(
        int state)
    {
        return openingInitial(state) | INITIAL_OPENED;
    }

    static int closedInitial(
        int state)
    {
        return state | INITIAL_CLOSED;
    }

    static boolean initialClosed(
        int state)
    {
        return (state & INITIAL_CLOSED) != 0;
    }

    static int openedReply(
        int state)
    {
        return state | REPLY_OPENED;
    }

    static int closingReply(
        int state)
    {
        return state | REPLY_CLOSING;
    }

    static int closedReply(
        int state)
    {
        return closingReply(state) | REPLY_CLOSED;
    }

    static boolean replyOpened(
        int state)
    {
        return (state & REPLY_OPENED) != 0;
    }

    static boolean replyClosing(
        int state)
    {
        return (state & REPLY_CLOSING) != 0;
    }

    static boolean replyClosed(
        int state)
    {
        return (state & REPLY_CLOSED) != 0;
    }

    private HttpCacheRequestState()
    {
        // utility
    }
}
