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
package org.reaktivity.nukleus.http_cache.internal.routable;

import java.util.Objects;
import java.util.function.Predicate;

public class Route
{
    private final String source;
    private final long sourceRef;
    private final String targetName;
    private final long targetRef;

    public Route(
        String source,
        long sourceRef,
        String targetName,
        long targetRef)
    {
        this.source = source;
        this.sourceRef = sourceRef;
        this.targetName = targetName;
        this.targetRef = targetRef;
    }

    public String source()
    {
        return source;
    }

    public long sourceRef()
    {
        return sourceRef;
    }

    public String targetName()
    {
        return targetName;
    }

    public long targetRef()
    {
        return this.targetRef;
    }

    @Override
    public int hashCode()
    {
        int result = source.hashCode();
        result = 31 * result + Long.hashCode(sourceRef);
        result = 31 * result + targetName.hashCode();
        result = 31 * result + Long.hashCode(targetRef);

        return result;
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Route))
        {
            return false;
        }

        Route that = (Route) obj;
        return this.sourceRef == that.sourceRef &&
                this.targetRef == that.targetRef &&
                Objects.equals(this.source, that.source) &&
                Objects.equals(this.targetName, that.targetName);
    }

    @Override
    public String toString()
    {
        return String.format("[source=\"%s\", sourceRef=%d, target=\"%s\", targetRef=%d]",
                source, sourceRef, targetName, targetRef);
    }

    public static Predicate<Route> sourceMatches(
        String source)
    {
        Objects.requireNonNull(source);
        return r -> source.equals(r.source);
    }

    public static Predicate<Route> sourceRefMatches(
        long sourceRef)
    {
        return r -> sourceRef == r.sourceRef;
    }

    public static Predicate<Route> targetMatches(
        String target)
    {
        Objects.requireNonNull(target);
        return r -> target.equals(r.targetName);
    }

    public static Predicate<Route> targetRefMatches(
        long targetRef)
    {
        return r -> targetRef == r.targetRef;
    }
}
