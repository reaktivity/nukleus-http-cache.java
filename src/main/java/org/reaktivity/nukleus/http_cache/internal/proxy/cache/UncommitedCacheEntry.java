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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class UncommitedCacheEntry implements CacheEntry
{

    private List<OnUpdateRequest> subscribeToUpdates = new ArrayList<OnUpdateRequest>();
    private CacheableRequest request;
    private Cache cache;

    public UncommitedCacheEntry(
        Cache cache,
        CacheableRequest request)
    {
        this.cache = cache;
        this.request = request;
    }

    @Override
    public void serveClient(AnswerableByCacheRequest request)
    {
        MessageConsumer acceptReply = request.acceptReply();
        long acceptReplyStreamId = request.acceptReplyStreamId();
        long acceptCorrelationId = request.acceptCorrelationId();
        cache.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
    }

    @Override
    public void cleanUp()
    {
        // NOOP
    }

    @Override
    public boolean canServeRequest(ListFW<HttpHeaderFW> request, short authScope)
    {
        return false;
    }

    @Override
    public boolean isUpdateRequestForThisEntry(ListFW<HttpHeaderFW> requestHeaders)
    {
        return CacheUtils.isMatchByEtag(requestHeaders, this.request.etag());
    }

    @Override
    public boolean subscribeToUpdate(OnUpdateRequest onModificationRequest)
    {
        if (request.state() ==  CacheableRequest.CacheState.COMMITING)
        {
            this.subscribeToUpdates.add(onModificationRequest);
            return true;
        }
        return false;
    }

    @Override
    public Stream<OnUpdateRequest> subscribersOnUpdate()
    {
        return this.subscribeToUpdates.stream();
    }

    @Override
    public boolean isUpdateBy(CacheableRequest request)
    {
        return true;
    }

    @Override
    public void refresh(AnswerableByCacheRequest request)
    {
        new IllegalStateException();
    }

    @Override
    public void abortSubscribers()
    {
        subscribersOnUpdate().forEach(s ->
        {
            MessageConsumer acceptReply = s.acceptReply();
            long acceptReplyStreamId = s.acceptReplyStreamId();
            long acceptCorrelationId = s.acceptCorrelationId();
            cache.writer.do503AndAbort(acceptReply, acceptReplyStreamId, acceptCorrelationId);
        });
    }
}
