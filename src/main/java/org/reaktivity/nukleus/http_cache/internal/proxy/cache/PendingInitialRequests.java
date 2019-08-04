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


import org.reaktivity.nukleus.http_cache.internal.proxy.request.DefaultRequest;

import java.util.ArrayList;
import java.util.List;

public class PendingInitialRequests
{
    private final DefaultRequest request;
    private final List<DefaultRequest> subscribers = new ArrayList<>();

    PendingInitialRequests(
        DefaultRequest request)
    {
        this.request = request;
        this.subscribers.add(request);
    }

    public DefaultRequest initialRequest()
    {
        return request;
    }

    public String etag()
    {
        return request.etag();
    }

    public void subscribe(DefaultRequest request)
    {
        this.subscribers.add(request);
    }

    public int numberOfSubscribers()
    {
        return this.subscribers.size();
    }

    public List<DefaultRequest> subcribers()
    {
        return this.subscribers;
    }

    void removeSubscriber(DefaultRequest request)
    {
        subscribers.remove(request);
    }

    PendingInitialRequests withNextInitialRequest()
    {
        if (subscribers.isEmpty())
        {
            return null;
        }

        final DefaultRequest newInitialRequest = subscribers.remove(0);
        final PendingInitialRequests newPendingRequests = new PendingInitialRequests(newInitialRequest);
        subscribers.forEach(newPendingRequests::subscribe);
        subscribers.clear();

        return newPendingRequests;
    }
}
