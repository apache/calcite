/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.runtime;

import java.util.*;


/**
 * <code>CompoundParallelTupleIter</code> creates one TupleIter out of several.
 * Unlike its serial counterpart {@link CompoundTupleIter}, it runs all its
 * inputs in parallel, in separate threads that it spawns. It outputs the next
 * element available from any of its inputs. Note that the order of output rows
 * is indeterminate, since it is unpredictable which input will arrive next.
 *
 * <p>The compound TupleIter is finished when all of its inputs are finished.
 * The set of input iterators is fixed at construction.
 *
 * <p>This variant is needed when an input is infinite, since CompoundTupleIter
 * would hang. Extending this class to preserve order is problematic, given its
 * low level:
 *
 * <ul>
 * <li>items Are now synthetic {@link Object}s.</li>
 * <li>Items would have to become things that expose a {@link Comparable} <i>
 * key</i> value.</li>
 * <li>Even if one input lags behind the other provding a {@link
 * Iterator#next()} value, that missing value might sort before its available
 * counterparts from the other inputs. There is no basis to decide to wait for
 * it or not.</li>
 * </ul>
 *
 * @author Marc Berkowitz
 */
public class CompoundParallelTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final TupleIter [] in;
    private final boolean [] endOfData;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundParallelTupleIter(TupleIter [] tupleIters)
    {
        this.in = tupleIters;
        this.endOfData = new boolean[tupleIters.length];
        this.i = -1;
    }

    //~ Methods ----------------------------------------------------------------

    public Object fetchNext()
    {
        int endOfDataCount = 0;

        final int N = in.length;

        for (int offset = 0; offset < N; offset++) {
            if (++i >= N) {
                i = 0;
            }

            if (endOfData[i]) {
                endOfDataCount++;
            } else {
                Object o = in[i].fetchNext();

                if (o == NoDataReason.END_OF_DATA) {
                    endOfData[i] = true;
                    endOfDataCount++;
                } else if (o == NoDataReason.UNDERFLOW) {
                    // Ignore this.
                } else {
                    return o;
                }
            }
        }

        if (endOfDataCount == N) {
            return NoDataReason.END_OF_DATA;
        }

        return NoDataReason.UNDERFLOW;
    }

    public void restart()
    {
        for (int index = 0; index < in.length; index++) {
            in[index].restart();
            endOfData[index] = false;
        }
        i = -1;
    }

    public void closeAllocation()
    {
        for (int index = 0; index < in.length; index++) {
            in[index].closeAllocation();
        }
    }
}

// End CompoundParallelTupleIter.java
