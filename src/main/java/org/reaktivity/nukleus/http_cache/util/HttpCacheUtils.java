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
package org.reaktivity.nukleus.http_cache.util;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableList;
import static org.reaktivity.nukleus.http_cache.util.HttpHeadersUtil.getHeader;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpCacheUtils
{

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    public static final List<String> CACHEABLE_BY_DEFAULT_STATUS_CODES = unmodifiableList(
            asList("200", "203", "204", "206", "300", "301", "404", "405", "410", "414", "501"));

    private HttpCacheUtils()
    {
        // utility class
    }

    public static boolean canBeServedByCache(
        ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
                case "cache-control":
                    if (value.contains("no-cache"))
                    {
                        return false;
                    }
                    return true;
                case ":method":
                    if ("GET".equalsIgnoreCase(value))
                    {
                        return false;
                    }
                    return true;
                default:
                    return false;
                }
        });
    }

    public static boolean responseCanSatisfyRequest(
            final ListFW<HttpHeaderFW> pendingRequestHeaders,
            final ListFW<HttpHeaderFW> myRequestHeaders,
            final ListFW<HttpHeaderFW> responseHeaders)
    {

        final String vary = getHeader(responseHeaders, "vary");
        final String cacheControl = getHeader(responseHeaders, "cache-control");

        final String pendingRequestAuthorizationHeader = getHeader(pendingRequestHeaders, "authorization");

        final String myAuthorizationHeader = getHeader(myRequestHeaders, "authorization");

        boolean useSharedResponse = true;

        if (cacheControl != null && cacheControl.contains("public"))
        {
            useSharedResponse = true;
        }
        else if (cacheControl != null && cacheControl.contains("private"))
        {
            useSharedResponse = false;
        }
        else if (myAuthorizationHeader != null || pendingRequestAuthorizationHeader != null)
        {
            useSharedResponse = false;
        }
        else if (vary != null)
        {
            useSharedResponse = stream(vary.split("\\s*,\\s*")).anyMatch(v ->
            {
                String pendingHeaderValue = getHeader(pendingRequestHeaders, v);
                String myHeaderValue = getHeader(myRequestHeaders, v);
                return Objects.equals(pendingHeaderValue, myHeaderValue);
            });
        }

        return useSharedResponse;
    }

    public static boolean isExpired(ListFW<HttpHeaderFW> responseHeaders)
    {
        String dateHeader = getHeader(responseHeaders, "date");
        if (dateHeader == null)
        {
            dateHeader = getHeader(responseHeaders, "last-modified");
        }
        if (dateHeader == null)
        {
            // invalid response, so say it is expired
            return true;
        }
        try
        {
            Date receivedDate = DATE_FORMAT.parse(dateHeader);
            String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, "cache-control");
            String ageExpires = null;
            if (cacheControl != null)
            {
                CacheControlParser parsedCacheControl = new CacheControlParser(cacheControl);
                ageExpires = parsedCacheControl.getValue("s-maxage");
                if (ageExpires == null)
                {
                    ageExpires = parsedCacheControl.getValue("max-age");
                }
            }
            int ageExpiresInt;
            if (ageExpires == null)
            {
                String lastModified = getHeader(responseHeaders, "last-modified");
                if (lastModified == null)
                {
                    ageExpiresInt = 5000; // default to 5
                }
                else
                {
                    Date lastModifiedDate = DATE_FORMAT.parse(lastModified);
                    ageExpiresInt = (int) ((receivedDate.getTime() - lastModifiedDate.getTime()) * (10.0f/100.0f));
                }
            }
            else
            {
                ageExpiresInt = Integer.parseInt(ageExpires) * 1000;
            }
            final Date expires = new Date(System.currentTimeMillis() - ageExpiresInt);
            boolean expired = expires.after(receivedDate);
            return expired;
        }
        catch (Exception e)
        {
            // Error so just expire it
            return true;
        }
    }

    // Apache Version 2.0 (July 25, 2017)
    // https://svn.apache.org/repos/asf/abdera/java/trunk/
    // core/src/main/java/org/apache/abdera/protocol/util/CacheControlUtil.java
    // TODO GC free
    public static class CacheControlParser implements Iterable<String>
    {

        private static final String REGEX =
            "\\s*([\\w\\-]+)\\s*(=)?\\s*(\\d+|\\\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)+\\\")?\\s*";

        private static final Pattern CACHE_DIRECTIVES = Pattern.compile(REGEX);

        private HashMap<String, String> values = new HashMap<String, String>();

        public CacheControlParser(String value)
        {
            values.clear();
            Matcher matcher = CACHE_DIRECTIVES.matcher(value);
            while (matcher.find())
            {
                String directive = matcher.group(1);
                values.put(directive, matcher.group(3));
            }
        }

        public Iterator<String> iterator()
        {
            return values.keySet().iterator();
        }

        public Map<String, String> getValues()
        {
            return values;
        }

        public String getValue(String directive)
        {
            return values.get(directive);
        }

        public List<String> getValues(String directive)
        {
            String values = getValue(directive);
            if (values != null)
            {
                return Arrays
                        .stream(values.split(","))
                        .map(v -> v.trim())
                        .collect(Collectors.toList());
            }
            return null;
        }

    }

}
