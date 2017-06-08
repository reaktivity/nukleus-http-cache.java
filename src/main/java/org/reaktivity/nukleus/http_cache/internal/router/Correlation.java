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
package org.reaktivity.nukleus.http_cache.internal.router;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class Correlation
{
    private final long id;
    private final String source;
    private final RouteKind established;
    private final int requestURLHash;

    public Correlation(
        long id,
        String source,
        RouteKind established,
        int requestURLHash)
    {
        this.id = id;
        this.source = requireNonNull(source, "source");
        this.established = requireNonNull(established, "established");
        this.requestURLHash = requestURLHash;
    }

    public long id()
    {
        return id;
    }

    public String source()
    {
        return source;
    }

    public RouteKind established()
    {
        return established;
    }

    public int requestURLHash()
    {
        return requestURLHash;
    }

    @Override
    public int hashCode()
    {
        int result = Long.hashCode(id);
        result = 31 * result + source.hashCode();
        result = 31 * result + established.hashCode();

        return result;
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.id == that.id &&
                this.established == that.established &&
                this.requestURLHash == that.requestURLHash &&
                Objects.equals(this.source, that.source);
    }

    @Override
    public String toString()
    {
        return String.format("[id=%d, source=\"%s\", established=%s, requestURLHash=%s]",
                id, source, established, requestURLHash);
    }

}
