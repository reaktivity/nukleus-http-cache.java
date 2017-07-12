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
package org.reaktivity.nukleus.http_cache.internal;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.ProxyStreamFactory.ProxyAcceptStream;

public class Correlation
{
    private final String acceptName;
    private final int slotIndex;
    private final int slotLimit;
    private long acceptCorrelation;
    private final BufferPool bufferPool;
    private final int requestURLHash;
    private final Int2ObjectHashMap<List<ProxyAcceptStream>> awaitingRequestMatches;

    public Correlation(
        String acceptName,
        BufferPool bufferPool,
        int slotIndex,
        int slotLimit,
        long acceptCorrelation,
        int requestURLHash,
        Int2ObjectHashMap<List<ProxyAcceptStream>> awaitingRequestMatches
    )
    {
        this.acceptName = requireNonNull(acceptName);
        this.slotIndex = slotIndex;
        this.slotLimit = slotLimit;
        this.acceptCorrelation = acceptCorrelation;
        this.bufferPool = bufferPool;
        this.requestURLHash = requestURLHash;
        this.awaitingRequestMatches = awaitingRequestMatches;
    }

    public String acceptName()
    {
        return acceptName;
    }

    public long acceptCorrelation()
    {
        return acceptCorrelation;
    }

    public int slotIndex()
    {
        return slotIndex;
    }

    public int slotLimit()
    {
        return slotLimit;
    }

    public BufferPool bufferPool()
    {
        return bufferPool;
    }

    public int requestURLHash()
    {
        return requestURLHash;
    }

    @Override
    public int hashCode()
    {
        int result = Long.hashCode(acceptCorrelation);
        result = 31 * result + acceptName.hashCode();
        result = 31 * result + slotIndex;
        result = 31 * result + slotLimit;
        result = 31 * result + bufferPool.hashCode();
        result = 31 * result + requestURLHash;
        result = 31 * result + awaitingRequestMatches.hashCode();

        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.acceptCorrelation == that.acceptCorrelation &&
                this.slotIndex == that.slotIndex &&
                this.slotLimit == that.slotLimit &&
                Objects.equals(this.acceptName, that.acceptName) &&
                Objects.equals(this.bufferPool, that.bufferPool) &&
                this.requestURLHash == that.requestURLHash &&
                this.awaitingRequestMatches.equals(that.awaitingRequestMatches);
    }

    @Override
    public String toString()
    {
        return String.format("[connectCorrelation=%d, slotIndex=%d slotLimit=%d connectSource=\"%s\", bufferPool=%s]",
                acceptCorrelation, slotIndex, slotLimit, acceptName, bufferPool);
    }

    public Int2ObjectHashMap<List<ProxyAcceptStream>> awaitingRequestMatches()
    {
        return awaitingRequestMatches;
    }

}
