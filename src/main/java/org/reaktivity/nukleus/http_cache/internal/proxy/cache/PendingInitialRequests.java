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
package org.reaktivity.nukleus.http_cache.internal.proxy.cache;


import org.reaktivity.nukleus.http_cache.internal.stream.HttpCacheProxyCacheableRequest;

import java.util.ArrayList;
import java.util.List;

public class PendingInitialRequests
{
    private final HttpCacheProxyCacheableRequest request;
    private final List<HttpCacheProxyCacheableRequest> subscribers = new ArrayList<>();

    PendingInitialRequests(
        HttpCacheProxyCacheableRequest request)
    {
        this.request = request;
        this.subscribers.add(request);
    }

    public HttpCacheProxyCacheableRequest initialRequest()
    {
        return request;
    }

    public String etag()
    {
        return request.etag();
    }

    public void subscribe(HttpCacheProxyCacheableRequest request)
    {
        this.subscribers.add(request);
    }

    public int numberOfSubscribers()
    {
        return this.subscribers.size();
    }

    public List<HttpCacheProxyCacheableRequest> subcribers()
    {
        return this.subscribers;
    }

    void removeSubscriber(HttpCacheProxyCacheableRequest request)
    {
        request.purge();
        subscribers.remove(request);
    }

    void removeAllSubscribers()
    {
        subscribers.clear();
    }

    PendingInitialRequests withNextInitialRequest()
    {
        if (subscribers.isEmpty())
        {
            return null;
        }

        final HttpCacheProxyCacheableRequest newInitialRequest = subscribers.remove(0);
        final PendingInitialRequests newPendingRequests = new PendingInitialRequests(newInitialRequest);
        subscribers.forEach(newPendingRequests::subscribe);
        subscribers.clear();

        return newPendingRequests;
    }
}
