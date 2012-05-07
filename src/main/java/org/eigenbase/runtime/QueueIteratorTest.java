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

import junit.framework.*;

import org.eigenbase.util.*;


/**
 * Test case for {@link QueueIterator} and its subclasses {@link ThreadIterator}
 * and {@link TimeoutQueueIterator}.
 */
public class QueueIteratorTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Multiplier which determines how long each logical clock tick lasts, and
     * therefore how fast the test is run. If you are getting sporadic problems,
     * raise the value. 100 seems to be too low; 200 seems to be OK on my 1.8GHz
     * laptop.
     */
    private static final int tickMillis = 1000;

    /**
     * Contents of queue in both tests.
     */
    private static final String [] queueValues = { "a", "b", null, "d" };

    //~ Instance fields --------------------------------------------------------

    /**
     * Timestamp at which the test started. All timeouts are relative to this.
     */
    private long startTime;

    //~ Constructors -----------------------------------------------------------

    public QueueIteratorTest(String s)
        throws Exception
    {
        super(s);
    }

    //~ Methods ----------------------------------------------------------------

    // NOTE jvs 21-Oct-2006:  I'm disabling this test because
    // it fails sporadically, and we're planning to eliminate
    // usage of this class anyway (http://issues.eigenbase.org/browse/FRG-168).
    public void testTimeoutIterator()
    {
        startTime = System.currentTimeMillis();

        // writer
        TickIterator tickIter = new TickIterator(queueValues, false, startTime);

        // reader
        TestingTimeoutQueueIterator ttqi =
            new TestingTimeoutQueueIterator(tickIter);

        ttqi.start();
        read(ttqi.getQueueIterator());
    }

    public void testThreadIterator()
    {
        startTime = System.currentTimeMillis();

        // reader and writer
        ThreadIterator threadIter =
            new ThreadIterator() {
                // paced data source
                TickIterator tickIter =
                    new TickIterator(queueValues, false, startTime);

                protected void doWork()
                {
                    while (tickIter.hasNext()) {
                        put(tickIter.next());
                    }
                }
            };

        threadIter.start();
        read(threadIter);
    }

    private void read(QueueIterator iter)
    {
        // tick 1: hasNext() returns true at tick 1
        // tick 2: next() returns "a"
        // tick 2: object is available
        assertHasNext(
            iter,
            true,
            toMillis(2.1));

        // call next with zero timeout -- it already has the answer
        assertNext(iter, "a", 0);

        // tick 3: hasNext returns true at tick 3
        assertHasNextTimesOut(
            iter,
            toMillis(2.7));
        assertHasNextTimesOut(
            iter,
            toMillis(2.9));

        // tick 4: next returns "b"
        // tick 4: object is available
        assertNextTimesOut(
            iter,
            toMillis(3.3));

        // call next with zero timeout will timeout immediately (not the
        // same as JDBC ResultSet.setQueryTimeout(0), which means don't
        // timeout ever)
        assertNextTimesOut(iter, 0);
        assertNextTimesOut(
            iter,
            toMillis(3.6));
        assertNextTimesOut(
            iter,
            toMillis(3.8));
        assertNext(
            iter,
            "b",
            toMillis(4.2));

        // tick 5: hasNext returns true
        // tick 6: next returns null (does not mean end of data)
        // tick 6: object is available
        // tick 7: hasNext returns true
        // tick 8: next returns "d"
        // tick 8: object is available
        assertHasNext(
            iter,
            true,
            toMillis(8.1));

        // call hasNext twice in succession
        assertHasNext(
            iter,
            true,
            toMillis(8.2));

        // call hasNext with zero timeout -- it already has the answer
        assertHasNext(iter, true, 0);

        // call hasNext with non-zero timeout -- it already has the answer
        assertHasNext(iter, true, 10);
        assertNext(
            iter,
            null,
            toMillis(8.2));

        // call next() without calling hasNext() is legal
        assertNext(
            iter,
            "d",
            toMillis(8.3));
        assertHasNextTimesOut(
            iter,
            toMillis(8.4));
        assertHasNextTimesOut(
            iter,
            toMillis(8.5));

        // tick 9: hasNext returns false
        // tick 9: no object is available
        assertHasNext(
            iter,
            false,
            toMillis(10.5));
        try {
            iter.next(100);
            fail("did not throw NoSuchElementException");
        } catch (QueueIterator.TimeoutException e) {
            fail("next() timed out");
        } catch (NoSuchElementException e) {
            // perfect
        }
    }

    private void assertHasNext(
        QueueIterator iter,
        boolean expected,
        long timeoutMillis)
    {
        try {
            boolean b = iter.hasNext(timeoutMillis);
            assertEquals(expected, b);
        } catch (QueueIterator.TimeoutException e) {
            fail("hasNext() timed out at " + new Date());
        }
    }

    private void assertHasNextTimesOut(
        QueueIterator iter,
        long timeoutMillis)
    {
        try {
            if (false) {
                System.out.println(
                    "entering hasNext at " + new Date()
                    + " with " + timeoutMillis);
            }
            boolean b = iter.hasNext(timeoutMillis);
            fail(
                "hasNext() returned " + b + " and did not time out at "
                + new Date());
        } catch (QueueIterator.TimeoutException e) {
            // success -- we timed out
        }
    }

    private void assertNext(
        QueueIterator iter,
        Object expected,
        long timeoutMillis)
    {
        try {
            Object actual = iter.next(timeoutMillis);
            assertEquals(expected, actual);
        } catch (QueueIterator.TimeoutException e) {
            fail("next() timed out at " + new Date());
        }
    }

    private void assertNextTimesOut(
        QueueIterator iter,
        long timeoutMillis)
    {
        try {
            Object o = iter.next(timeoutMillis);
            Util.discard(o);
            fail("next() did not time out at " + new Date());
        } catch (QueueIterator.TimeoutException e) {
            // success -- we timed out
        }
    }

    private long toMillis(double tick)
    {
        long endTime = startTime + (long) (tick * tickMillis);
        return endTime - System.currentTimeMillis();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A TimeoutQueueIterator that exposes its inner QueueIterator for direct
     * reading. Just a kludge to fit this test.
     */
    private static class TestingTimeoutQueueIterator
        extends TimeoutQueueIterator
    {
        TestingTimeoutQueueIterator(Iterator producer)
        {
            super(producer);
        }

        public QueueIterator getQueueIterator()
        {
            return queueIterator;
        }
    }

    /**
     * Iterator which returns an element from an array on a regular basis.
     *
     * <p>Every clock tick until the array is exhausted, {@link #hasNext}
     * returns true, then the following clock tick, {@link #next} returns an
     * object. If you call a method too early, the method waits until the
     * appropriate time.
     */
    private static class TickIterator
        implements Iterator
    {
        private final boolean verbose;
        private final long startTime;
        private int current;
        private final Object [] values;

        TickIterator(
            Object [] values,
            boolean verbose,
            long startTime)
        {
            this.values = values;
            this.verbose = verbose;
            this.startTime = startTime;
        }

        public boolean hasNext()
        {
            int tick = (current * 2) + 1;
            waitUntil(tick);
            if (current < values.length) {
                if (verbose) {
                    System.out.println(
                        new Date() + " (tick " + tick
                        + ") hasNext returns true");
                }
                return true;
            } else {
                if (verbose) {
                    System.out.println(
                        new Date() + " (tick " + tick
                        + ") hasNext returns false");
                }
                return false;
            }
        }

        private void waitUntil(int tick)
        {
            long timeToWait =
                (startTime + (tick * QueueIteratorTest.tickMillis))
                - System.currentTimeMillis();
            if (timeToWait > 0) {
                try {
                    Thread.sleep(timeToWait);
                } catch (InterruptedException e) {
                }
            }
        }

        public Object next()
        {
            int tick = (current * 2) + 2;
            waitUntil(tick);
            Object value = values[current];
            if (verbose) {
                System.out.println(
                    new Date() + " (tick " + tick + ") return "
                    + value);
            }
            ++current;
            return value;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public static void demo()
        {
            String [] values = { "a", "b", "c" };
            TickIterator tickIterator =
                new TickIterator(
                    values,
                    true,
                    System.currentTimeMillis());
            while (tickIterator.hasNext()) {
                Util.discard(tickIterator.next());
            }
        }
    }
}

// End QueueIteratorTest.java
