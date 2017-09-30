package org.reaktivity.nukleus.http_cache.internal.stream.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// Apache Version 2.0 (July 25, 2017)
// https://svn.apache.org/repos/asf/abdera/java/trunk/
// core/src/main/java/org/apache/abdera/protocol/util/CacheControlUtil.java

public class CacheControl
{
    private static final String REGEX =
            "\\s*([\\w\\-]+)\\s*(=)?\\s*(\\d+|\\\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)+\\\")?\\s*";

    private static final Pattern CACHE_DIRECTIVES = Pattern.compile(REGEX);

    private HashMap<String, String> values = new HashMap<>();

    public CacheControl()
    {

    }

    public CacheControl parse(String value)
    {
        values.clear();
        if (value != null)
        {
            Matcher matcher = CACHE_DIRECTIVES.matcher(value);
            while (matcher.find())
            {
                String directive = matcher.group(1);
                values.put(directive, matcher.group(3));
            }
        }
        return this;
    }

    public Iterator<String> iterator()
    {
        return values.keySet().iterator();
    }

    public Map<String, String> getValues()
    {
        return values;
    }

    public boolean contains(String directive)
    {
        return values.containsKey(directive);
    }

    public String getValue(String directive)
    {
        return values.get(directive);
    }

    public List<String> getValues(String directive)
    {
        String dValues = getValue(directive);
        if (dValues != null)
        {
            return Arrays
                    .stream(dValues.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }
        return null;
    }
}
