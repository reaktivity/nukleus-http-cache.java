package org.reaktivity.nukleus.http_cache.internal.test;

import static org.junit.Assert.assertEquals;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class HttpCacheCountersRule implements TestRule
{
    private static final int NUM_OF_SLOTS_PER_CACHE_ENTRY = 2;
    private final ReaktorRule reaktor;

    public HttpCacheCountersRule(ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                HttpCacheController controller = controller();
                assertEquals(0, controller.count("streams"));
                assertEquals(0, controller.count("routes"));
                assertEquals(0, controller.count("enqueues"));
                assertEquals(0, controller.count("dequeues"));
                base.evaluate();
                assertEquals(controller.count("enqueues"), controller.count("dequeues"));
            }

        };
    }

    public long slabAquires()
    {
        return controller().count("slab.aquires");
    }

    public long slabReleases()
    {
        return controller().count("slab.releases");
    }

    private HttpCacheController controller()
    {
        return reaktor.controller(HttpCacheController.class);
    }

    public void assertNumOfCacheResponsesEquals(int numberOfResponses)
    {
        assertEquals(NUM_OF_SLOTS_PER_CACHE_ENTRY * numberOfResponses, slabAquires() - slabReleases());
    }
}
