/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.test;

import java.util.*;
import java.util.regex.*;

import junit.framework.*;

import org.eigenbase.runtime.*;


public abstract class EigenbaseTestCase
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final String nl = System.getProperty("line.separator");
    protected static final String [] emptyStringArray = new String[0];

    //~ Constructors -----------------------------------------------------------

    protected EigenbaseTestCase(String s)
        throws Exception
    {
        super(s);
    }

    //~ Methods ----------------------------------------------------------------

    protected static void assertEqualsDeep(
        Object o,
        Object o2)
    {
        if ((o instanceof Object []) && (o2 instanceof Object [])) {
            Object [] a = (Object []) o;
            Object [] a2 = (Object []) o2;
            assertEquals(a.length, a2.length);
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
            assertEquals(o, o2);
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
     * Returns a TupleIter over the elements of an array.
     */
    public static TupleIter makeTupleIter(final Object [] a)
    {
        return new AbstractTupleIter() {
            private List data = Arrays.asList(a);
            private Iterator iter = data.iterator();

            public Object fetchNext()
            {
                if (iter.hasNext()) {
                    return iter.next();
                }

                return NoDataReason.END_OF_DATA;
            }

            public void restart()
            {
                iter = data.iterator();
            }

            public void closeAllocation()
            {
                iter = null;
                data = null;
            }
        };
    }

    /**
     * Converts an iterator to a list.
     */
    protected static List toList(Iterator iterator)
    {
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    /**
     * Converts a TupleIter to a list.
     */
    protected static List toList(TupleIter tupleIter)
    {
        ArrayList list = new ArrayList();
        while (true) {
            Object o = tupleIter.fetchNext();
            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                return list;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                // Busy loops.
                continue;
            }

            list.add(o);
        }
    }

    /**
     * Converts an enumeration to a list.
     */
    protected static List toList(Enumeration enumeration)
    {
        ArrayList list = new ArrayList();
        while (enumeration.hasMoreElements()) {
            list.add(enumeration.nextElement());
        }
        return list;
    }

    /**
     * Checks that an iterator returns the same objects as the contents of an
     * array.
     */
    protected void assertEquals(
        Iterator iterator,
        Object [] a)
    {
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        assertEquals(list, a);
    }

    /**
     * Checks that a TupleIter returns the same objects as the contents of an
     * array.
     */
    protected void assertEquals(
        TupleIter iterator,
        Object [] a)
    {
        ArrayList list = new ArrayList();
        while (true) {
            Object next = iterator.fetchNext();
            if (next == TupleIter.NoDataReason.END_OF_DATA) {
                break;
            }
            list.add(next);
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
            assertEquals(expected[i], actual[i]);
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
            assertEquals((Object) expected, actual);
        }
    }

    /**
     * Copies all of the tests in a suite whose names match a given pattern.
     */
    public static TestSuite copySuite(
        TestSuite suite,
        Pattern testPattern)
    {
        TestSuite newSuite = new TestSuite();
        Enumeration tests = suite.tests();
        while (tests.hasMoreElements()) {
            Test test = (Test) tests.nextElement();
            if (test instanceof TestCase) {
                TestCase testCase = (TestCase) test;
                final String testName = testCase.getName();
                if (testPattern.matcher(testName).matches()) {
                    newSuite.addTest(test);
                }
            } else if (test instanceof TestSuite) {
                TestSuite subSuite = copySuite((TestSuite) test, testPattern);
                if (subSuite.countTestCases() > 0) {
                    newSuite.addTest(subSuite);
                }
            } else {
                // some other kind of test
                newSuite.addTest(test);
            }
        }
        return newSuite;
    }
}

// End EigenbaseTestCase.java
