package org.reaktivity.nukleus.http_cache.internal.proxy.request;

public class CacheRefreshRequest extends CacheableRequest
{

    public CacheRefreshRequest(
            CacheableRequest req,
            int requestSlot)
    {
        // TODO eliminate reference /GC duplication (Flyweight pattern?)
        super(req.acceptName,
              req.acceptReply,
              req.acceptReplyStreamId,
              req.acceptCorrelationId,
              req.connect,
              req.connectName,
              req.connectRef,
              req.supplyCorrelationId,
              req.supplyStreamId,
              req.requestURLHash,
              req.responseBufferPool,
              req.requestBufferPool,
              requestSlot,
              req.requestSize,
              req.router,
              req.authScope);
    }

    @Override
    public Type getType()
    {
        return Type.CACHE_REFRESH;
    }

}
