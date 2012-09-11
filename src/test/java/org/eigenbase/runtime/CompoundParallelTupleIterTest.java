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

import java.util.Arrays;

import org.eigenbase.test.EigenbaseTestCase;

/**
 * Unit test for {@link CompoundParallelTupleIter}.
 *
 * @author jhyde
 */
public class CompoundParallelTupleIterTest
    extends EigenbaseTestCase
{
    public CompoundParallelTupleIterTest(String s)
        throws Exception
    {
        super(s);
    }

    // The CompoundParallelTupleIter preserves the order of 2 elements
    // from the same source, but may transpose 2 elements from different
    // soureces. Being sloppy, just test that the actual results match the
    // expected results when resorted.
    protected void assertEquals(
        TupleIter tupleIter,
        Object [] expected) // expected vals -- sorted in place

    {
        Object [] actual = toList(tupleIter).toArray(); // get results
        Arrays.sort(actual);
        Arrays.sort(expected);
        assertEquals(expected, actual);
    }

    public void testCompoundParallelTupleIter2()
    {
        TupleIter tupleIter =
            new CompoundParallelTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] { "a", "b" }),
                    makeTupleIter(new String[] { "c" })
                });
        assertEquals(
            tupleIter,
            new String[] { "a", "b", "c" });
    }

    public void testCompoundParallelTupleIter1()
    {
        TupleIter tupleIter =
            new CompoundParallelTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] { "a", "b", "c" })
                });
        assertEquals(
            tupleIter,
            new String[] { "a", "b", "c" });
    }

    public void testCompoundParallelTupleIter3()
    {
        TupleIter tupleIter =
            new CompoundParallelTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] { "a", "b", "c" }),
                    makeTupleIter(new String[] { "d", "e" }),
                    makeTupleIter(new String[] { "f" }),
                });
        assertEquals(
            tupleIter,
            new String[] { "a", "b", "c", "d", "e", "f" });
    }

    public void testCompoundParallelIterEmpty1()
    {
        TupleIter tupleIter =
            new CompoundParallelTupleIter(new TupleIter[] {});
        assertEquals(
            tupleIter,
            new String[] {});
    }

    public void testCompoundParallelIterEmpty2()
    {
        TupleIter tupleIter =
            new CompoundParallelTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] {}),
                    makeTupleIter(new String[] { "a", "b" })
                });
        assertEquals(
            tupleIter,
            new String[] { "a", "b" });
    }
}

// End CompoundParallelTupleIterTest.java
