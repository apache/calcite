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
package org.eigenbase.util;

import java.util.*;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link ArrayQueue}.
 */
public class ArrayQueueTest {
    //~ Constructors -----------------------------------------------------------

    public ArrayQueueTest() {
    }

    //~ Methods ----------------------------------------------------------------

    @Test public void testOfferPoll() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        assertTrue(queue.offer("1"));
        assertTrue(queue.offer("2"));
        assertTrue(queue.offer("3"));

        assertEquals(
            3,
            queue.size());

        assertEquals(
            "1",
            queue.poll());
        assertEquals(
            "2",
            queue.poll());
        assertEquals(
            "3",
            queue.poll());

        assertNull(queue.poll());
        assertEquals(
            0,
            queue.size());
        assertTrue(queue.isEmpty());
    }

    @Test public void testRepeatedOfferPoll() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        for (int i = 1; i < 1000; i++) {
            for (int j = 0; j < i; j++) {
                assertTrue(
                    queue.offer(String.valueOf(i) + "_" + String.valueOf(j)));
            }

            assertEquals(
                i,
                queue.size());

            for (int j = 0; j < i; j++) {
                assertEquals(
                    String.valueOf(i) + "_" + String.valueOf(j),
                    queue.poll());
            }

            assertNull(queue.poll());
            assertEquals(
                0,
                queue.size());
            assertTrue(queue.isEmpty());
        }
    }

    @Test public void testEmptyAndClear() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        assertTrue(queue.isEmpty());
        assertEquals(
            0,
            queue.size());

        assertTrue(queue.offer("1"));
        assertTrue(queue.offer("2"));
        assertTrue(queue.offer("3"));
        assertTrue(queue.offer("4"));
        assertTrue(queue.offer("5"));

        assertEquals(
            5,
            queue.size());

        queue.clear();

        assertTrue(queue.isEmpty());
        assertEquals(
            0,
            queue.size());
    }

    @Test public void testAddAddAllRemove() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        queue.add("1");
        queue.add("2");
        queue.add("3");

        assertEquals(
            3,
            queue.size());

        assertEquals(
            "1",
            queue.remove());
        assertEquals(
            "2",
            queue.remove());
        assertEquals(
            "3",
            queue.remove());

        assertNull(queue.poll());
        assertEquals(
            0,
            queue.size());
        assertTrue(queue.isEmpty());

        ArrayList<String> list = new ArrayList<String>();
        list.add("1");
        list.add("2");
        list.add("3");

        queue = new ArrayQueue<String>();

        queue.addAll(list);

        assertEquals(
            3,
            queue.size());

        assertEquals(
            "1",
            queue.remove());
        assertEquals(
            "2",
            queue.remove());
        assertEquals(
            "3",
            queue.remove());

        assertNull(queue.poll());
        assertEquals(
            0,
            queue.size());
        assertTrue(queue.isEmpty());
    }

    @Test public void testExceptions() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        try {
            queue.add(null);

            fail();
        } catch (NullPointerException e) {
        }

        try {
            ArrayList<String> l = new ArrayList<String>();
            l.add("1");
            l.add(null);
            l.add("2");

            queue.addAll(l);

            fail();
        } catch (NullPointerException e) {
        }

        queue.clear();

        try {
            queue.addAll(queue);

            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            queue.remove();

            fail();
        } catch (NoSuchElementException e) {
        }

        try {
            queue.element();

            fail();
        } catch (NoSuchElementException e) {
        }
    }

    @Test public void testPeek() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        assertNull(queue.peek());

        assertTrue(queue.offer("1"));
        assertEquals(
            "1",
            queue.peek());

        assertTrue(queue.offer("2"));
        assertEquals(
            "1",
            queue.peek());

        assertTrue(queue.offer("3"));
        assertEquals(
            "1",
            queue.peek());
        assertEquals(
            "1",
            queue.poll());

        assertEquals(
            "2",
            queue.peek());
        assertEquals(
            "2",
            queue.poll());

        assertEquals(
            "3",
            queue.peek());
        assertEquals(
            "3",
            queue.poll());

        assertNull(queue.peek());
    }

    @Test public void testIterator() {
        ArrayQueue<String> queue = new ArrayQueue<String>();

        assertTrue(queue.offer("1"));
        assertTrue(queue.offer("2"));
        assertTrue(queue.offer("3"));

        assertEquals(
            3,
            queue.size());

        Iterator i = queue.iterator();
        assertNotNull(i);

        assertEquals(
            "1",
            queue.poll());
        assertEquals(
            "2",
            queue.poll());
        assertEquals(
            "3",
            queue.poll());

        assertNull(queue.poll());
        assertEquals(
            0,
            queue.size());
        assertTrue(queue.isEmpty());

        assertTrue(i.hasNext());
        assertEquals(
            "1",
            i.next());
        assertTrue(i.hasNext());
        assertEquals(
            "2",
            i.next());
        assertTrue(i.hasNext());
        assertEquals(
            "3",
            i.next());
        assertTrue(!i.hasNext());
    }

    @Test public void testConstructors() {
        try {
            new ArrayQueue<Integer>(0);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ArrayQueue<Integer>(-1);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test public void testEquals() {
        ArrayQueue<String> queue1 = new ArrayQueue<String>();
        queue1.add("1");
        queue1.add("2");
        queue1.add("3");

        ArrayList<String> list = new ArrayList<String>();
        list.add("1");
        list.add("2");
        list.add("3");
        ArrayQueue<String> queue2 = new ArrayQueue<String>(list);

        assertEquals(queue1, queue2);

        ArrayQueue<String> queue3 = new ArrayQueue<String>();
        queue2.add("3");
        queue2.add("2");
        queue2.add("1");

        assertTrue(!queue1.equals(queue3));
    }

    @Test public void testToString() {
        ArrayQueue<String> queue = new ArrayQueue<String>();
        queue.add("1");
        queue.add("2");
        queue.add("3");

        assertEquals(
            "[1, 2, 3]",
            queue.toString());
    }

    @Test public void testToArray() {
        ArrayQueue<String> queue = new ArrayQueue<String>();
        queue.add("1");
        queue.add("2");
        queue.add("3");

        assertEquals(
            Arrays.asList((Object) "1", "2", "3"),
            Arrays.asList(queue.toArray()));

        ArrayQueue<String> queue2 = new ArrayQueue<String>();
        queue2.add("a");
        queue2.add("b");
        queue2.add("c");

        assertEquals(
            Arrays.asList("a", "b", "c"),
            Arrays.asList(queue2.toArray(new String[3])));
    }

    @Test public void testOfferNull() {
        ArrayQueue<String> queue = new ArrayQueue<String>();
        assertTrue(!queue.offer(null));
        assertEquals(
            0,
            queue.size());

        assertTrue(queue.offer("1"));
        assertTrue(!queue.offer(null));
        assertTrue(queue.offer("2"));
        assertTrue(!queue.offer(null));
        assertTrue(queue.offer("3"));
        assertTrue(!queue.offer(null));

        assertEquals(
            3,
            queue.size());

        assertEquals(
            "1",
            queue.poll());
        assertEquals(
            "2",
            queue.poll());
        assertEquals(
            "3",
            queue.poll());

        assertNull(queue.poll());
        assertEquals(
            0,
            queue.size());
        assertTrue(queue.isEmpty());
    }
}

// End ArrayQueueTest.java
