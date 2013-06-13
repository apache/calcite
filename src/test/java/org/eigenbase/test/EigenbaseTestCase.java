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

import org.junit.Assert;

import static org.junit.Assert.*;


public abstract class EigenbaseTestCase {
    //~ Static fields/initializers ---------------------------------------------

    protected static final String nl = System.getProperty("line.separator");

    //~ Constructors -----------------------------------------------------------

    protected EigenbaseTestCase() {
    }

    //~ Methods ----------------------------------------------------------------

    protected static void assertEqualsDeep(
        Object o,
        Object o2)
    {
        if ((o instanceof Object []) && (o2 instanceof Object [])) {
            Object [] a = (Object []) o;
            Object [] a2 = (Object []) o2;
            Assert.assertEquals(a.length, a2.length);
            for (int i = 0; i < a.length; i++) {
                assertEqualsDeep(a[i], a2[i]);
            }
            return;
        }
        if ((o != null)
            && (o2 != null)
            && o.getClass().isArray()
            && (o.getClass() == o2.getClass()))
        {
            boolean eq;
            if (o instanceof boolean []) {
                eq = Arrays.equals((boolean []) o, (boolean []) o2);
            } else if (o instanceof byte []) {
                eq = Arrays.equals((byte []) o, (byte []) o2);
            } else if (o instanceof char []) {
                eq = Arrays.equals((char []) o, (char []) o2);
            } else if (o instanceof short []) {
                eq = Arrays.equals((short []) o, (short []) o2);
            } else if (o instanceof int []) {
                eq = Arrays.equals((int []) o, (int []) o2);
            } else if (o instanceof long []) {
                eq = Arrays.equals((long []) o, (long []) o2);
            } else if (o instanceof float []) {
                eq = Arrays.equals((float []) o, (float []) o2);
            } else if (o instanceof double []) {
                eq = Arrays.equals((double []) o, (double []) o2);
            } else {
                eq = false;
            }
            if (!eq) {
                fail("arrays not equal");
            }
        } else {
            // will handle the case 'o instanceof int[]' ok, because
            // shallow comparison is ok for ints
            Assert.assertEquals(o, o2);
        }
    }

    /**
     * Fails if <code>throwable</code> is null, or if its message does not
     * contain the string <code>pattern</code>.
     */
    protected void assertThrowableContains(
        Throwable throwable,
        String pattern)
    {
        if (throwable == null) {
            fail(
                "expected exception containing pattern <" + pattern
                + "> but got none");
        }
        String message = throwable.getMessage();
        if ((message == null) || (message.indexOf(pattern) < 0)) {
            fail(
                "expected pattern <" + pattern + "> in exception <"
                + throwable + ">");
        }
    }

    /**
     * Returns an iterator over the elements of an array.
     */
    public static Iterator makeIterator(Object [] a)
    {
        return Arrays.asList(a).iterator();
    }

    /**
     * Checks that an iterator returns the same objects as the contents of an
     * array.
     */
    protected void assertEquals(
        Iterator iterator,
        Object [] a)
    {
        List<Object> list = new ArrayList<Object>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        assertEquals(list, a);
    }

    /**
     * Checks that a list has the same contents as an array.
     */
    protected void assertEquals(
        List list,
        Object [] a)
    {
        Object [] b = list.toArray();
        assertEquals(a, b);
    }

    /**
     * Checks that two arrays are equal.
     */
    protected void assertEquals(
        Object [] expected,
        Object [] actual)
    {
        assertTrue(expected.length == actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], actual[i]);
        }
    }

    protected void assertEquals(
        Object [] expected,
        Object actual)
    {
        if (actual instanceof Object []) {
            assertEquals(expected, (Object []) actual);
        } else {
            // They're different. Let assertEquals(Object,Object) give the
            // error.
            Assert.assertEquals(expected, actual);
        }
    }
}

// End EigenbaseTestCase.java
