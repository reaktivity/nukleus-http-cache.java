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

import java.util.stream.Stream;

import org.reaktivity.nukleus.http_cache.internal.proxy.request.AnswerableByCacheRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.CacheableRequest;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.OnUpdateRequest;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public interface CacheEntry
{

    void serveClient(AnswerableByCacheRequest streamCorrelation);

    void cleanUp();

    boolean canServeRequest(ListFW<HttpHeaderFW> request, short authScope);

    boolean isUpdateRequestForThisEntry(ListFW<HttpHeaderFW> requestHeaders);

    boolean subscribeToUpdate(OnUpdateRequest onModificationRequest);

    Stream<OnUpdateRequest> subscribersOnUpdate();

    boolean isUpdateBy(CacheableRequest request);

    void refresh(AnswerableByCacheRequest request);

    void abortSubscribers();

    boolean expectSubscribers();

}