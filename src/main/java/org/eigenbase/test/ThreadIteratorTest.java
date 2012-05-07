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
package org.eigenbase.test;

import java.util.*;
import java.util.concurrent.*;

import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * Test for {@link ThreadIterator}.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class ThreadIteratorTest
    extends EigenbaseTestCase
{
    //~ Constructors -----------------------------------------------------------

    public ThreadIteratorTest(String s)
        throws Exception
    {
        super(s);
    }

    //~ Methods ----------------------------------------------------------------

    public void testBeatlesSynchronous()
    {
        testBeatles(null);
    }

    public void testBeatlesPipelined()
    {
        testBeatles(new ArrayBlockingQueue(2));
    }

    private void testBeatles(BlockingQueue queue)
    {
        Iterator beatles =
            new ThreadIterator(queue) {
                String [] strings;

                public ThreadIterator start(String [] strings)
                {
                    this.strings = strings;
                    return start();
                }

                protected void doWork()
                {
                    for (int i = 0; i < strings.length; i++) {
                        put(new Integer(strings[i].length()));
                    }
                }
            }.start(new String[] { "lennon", "mccartney", null, "starr" });
        assertTrue(beatles.hasNext());
        assertEquals(
            beatles.next(),
            new Integer(6));
        assertEquals(
            beatles.next(),
            new Integer(9));
        boolean barf = false;
        try {
            Util.discard(beatles.next());
        } catch (NullPointerException e) {
            barf = true;
        }
        assertTrue("expected a NullPointerException", barf);
    }

    public void testDigits()
    {
        ThreadIterator threadIterator =
            new ThreadIterator() {
                protected void doWork()
                {
                    for (int i = 0; i < 10; i++) {
                        put(new Integer(i));
                    }
                }
            };
        Iterator digits = threadIterator.iterator();
        assertEquals(
            digits,
            new Integer[] {
                new Integer(0), new Integer(1), new Integer(2),
                new Integer(3), new Integer(4), new Integer(5),
                new Integer(6), new Integer(7), new Integer(8),
                new Integer(9)
            });
        assertTrue(!digits.hasNext());
    }

    public void testEmpty()
    {
        Object [] empty = new Object[0];
        assertEquals(
            new ArrayIterator(empty),
            empty);
    }

    public void testXyz()
    {
        String [] xyz = new String[] { "x", "y", "z" };
        assertEquals(
            new ArrayIterator(xyz),
            xyz);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ArrayIterator
        extends ThreadIterator
    {
        Object [] a;

        ArrayIterator(Object [] a)
        {
            this.a = a;
            start();
        }

        protected void doWork()
        {
            for (int i = 0; i < a.length; i++) {
                this.put(a[i]);
            }
        }
    }
}
// End ThreadIteratorTest.java
