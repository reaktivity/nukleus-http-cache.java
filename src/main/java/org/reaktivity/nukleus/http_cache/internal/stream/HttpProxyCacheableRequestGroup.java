package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.collections.Int2ObjectCache;
import org.agrona.collections.Int2ObjectHashMap;

public class HttpProxyCacheableRequestGroup
{
     private final Int2ObjectHashMap requestGroups;

     public HttpProxyCacheableRequestGroup()
     {

         requestGroups = new Int2ObjectHashMap();
     }
}
