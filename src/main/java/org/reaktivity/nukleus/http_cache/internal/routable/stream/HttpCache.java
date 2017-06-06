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
package org.reaktivity.nukleus.http_cache.internal.routable.stream;

import static org.reaktivity.nukleus.http_cache.internal.util.function.HttpHeadersUtil.getRequestURL;

import java.util.Map;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

/**
 * A chunk of shared memory for temporary storage of data. This is logically segmented into a set of
 * slots of equal size. Methods are provided for acquiring a slot, getting a buffer that can be used
 * to store data in it, and releasing the slot once it is no longer needed.
 * <b>Each instance of this class is assumed to be used by one and only one thread.</b>
 */
public class HttpCache
{
    public final Varies all = new StarVaries();

    public final Long2LongHashMap urlToResponses = new Long2LongHashMap(-1);
    public final Long2LongHashMap urlToRequestHeaders = new Long2LongHashMap(-1);

    private long storeRequest(HttpBeginExFW fw)
    {
        // TODO
        return 0;
    }

    public final void request(HttpBeginExFW fw)
    {
        long slotId = storeRequest(fw);
        makeRequestOrDebounce(fw.headers(), slotId);
    }


    private void makeRequestOrDebounce(ListFW<HttpHeaderFW> headers, long slotId)
    {
        String url = getRequestURL(headers);
        boolean varies = variesFromStoredOrPending(url, headers);
        if(varies)
        {
            // TODO make request
        }
        else
        {
            // TODO tag on to other request
        }
    }

    private boolean variesFromStoredOrPending(String url, ListFW<HttpHeaderFW> headers)
    {
        Varies vary = null; // urlToVary.getOrDefault(url, all);
        return vary.varies(headers);
    }

    public static long hash(String string)
    {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++)
        {
            h = 31 * h + string.charAt(i);
        }
        return h;
    }

    public interface Varies
    {
        boolean varies(ListFW<HttpHeaderFW> requestHeaders);
    }
    // TODO, remove class and go to Flyweight around stored response
    public class DefaultVaries implements Varies
    {
        private final Map<String, String> headers;

        public DefaultVaries(Map<String, String> headers)
        {
            this.headers = headers;
        }

        @Override
        public boolean varies(ListFW<HttpHeaderFW> requestHeaders)
        {
            return headers.entrySet().stream().anyMatch(e ->
            {
                final String value = e.getValue();
                final String key = e.getKey();
                if(value == null)
                {
                    return requestHeaders.anyMatch(h -> key.equals(h.name().asString()));
                }
                else
                {
                    return requestHeaders.anyMatch(h -> key.equals(h.name().asString()) && value.equals(h.value().asString()));
                }
          });
        }
    }

    public class StarVaries implements Varies
    {

        public StarVaries()
        {
            // NOOP
        }
        @Override
        public boolean varies(ListFW<HttpHeaderFW> requestHeaders)
        {
            return true;
        }

    }
}