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
package org.eigenbase.runtime;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.test.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * <code>CompoundIterator</code> creates an iterator out of several.
 * CompoundIterator is serial: it yields all the elements of its first input
 * Iterator, then all those of its second input, etc. When all inputs are
 * exhausted, it is done.
 *
 * <p>NOTE jvs 21-Mar-2006: This class is no longer used except by Saffron, but
 * is generally useful. Should probably be moved to a utility package.
 */
public class CompoundIterator
    implements RestartableIterator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getCompoundIteratorTracer();

    //~ Instance fields --------------------------------------------------------

    private Iterator iterator;
    private Iterator [] iterators;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundIterator(Iterator [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        tracer.finer(toString());
        if (iterator == null) {
            return nextIterator();
        }
        if (iterator.hasNext()) {
            return true;
        }
        return nextIterator();
    }

    public Object next()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        return iterator.next();
    }

    public void remove()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        iterator.remove();
    }

    // moves to the next child iterator, skipping any empty ones, and returns
    // true. when all the child iteratators are used up, return false;
    private boolean nextIterator()
    {
        while (i < iterators.length) {
            iterator = iterators[i++];
            tracer.fine("try " + iterator);
            if (iterator.hasNext()) {
                return true;
            }
        }
        tracer.fine("exhausted iterators");
        iterator = Collections.EMPTY_LIST.iterator();
        return false;
    }

    // implement RestartableIterator
    public void restart()
    {
        for (int j = 0; j < i; ++j) {
            Util.restartIterator(iterators[j]);
        }
        i = 0;
        iterator = null;
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class Test
        extends EigenbaseTestCase
    {
        public Test(String s)
            throws Exception
        {
            super(s);
        }

        public void testCompoundIter()
        {
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        makeIterator(new String[] { "a", "b" }),
                        makeIterator(new String[] { "c" })
                    });
            assertEquals(
                iterator,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundIterEmpty()
        {
            Iterator iterator = new CompoundIterator(new Iterator[] {});
            assertEquals(
                iterator,
                new String[] {});
        }

        public void testCompoundIterFirstEmpty()
        {
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        makeIterator(new String[] {}),
                        makeIterator(new String[] { "a", null }),
                        makeIterator(new String[] {}),
                        makeIterator(new String[] {}),
                        makeIterator(new String[] { "b", "c" }),
                        makeIterator(new String[] {})
                    });
            assertEquals(
                iterator,
                new String[] { "a", null, "b", "c" });
        }

        /**
         * Checks that a BoxIterator returns the same values as the contents of
         * an array.
         */
        protected void assertUnboxedEquals(Iterator p, Object [] a)
        {
            ArrayList list = new ArrayList();
            while (p.hasNext()) {
                Object o = p.next();
                if (o instanceof Box) {
                    list.add(((Box) o).getValue());
                } else {
                    list.add(o);
                }
            }
            assertEquals(list, a);
        }

        public void testCompoundBoxIter()
        {
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        new BoxIterator(
                            makeIterator(
                                new String[] { "400", "401", "402", "403" })),
                        new BoxIterator(
                            makeIterator(
                                new String[] { "500", "501", "502", "503" })),
                        new BoxIterator(
                            makeIterator(
                                new String[] { "600", "601", "602", "603" }))
                    });
            assertUnboxedEquals(
                iterator,
                new String[] {
                    "400", "401", "402", "403",
                    "500", "501", "502", "503",
                    "600", "601", "602", "603"
                });
        }

        // a boxed value (see BoxIterator below)
        static class Box
        {
            Object val;

            public Box()
            {
                val = null;
            }

            public Object getValue()
            {
                return val;
            }

            public Box setValue(Object val)
            {
                this.val = val;
                return this;
            }
        }

        // An Iterator that always returns the same object, a Box, but with
        // different contents. Mimics the Iterator from a farrago dynamic
        // statement.
        static class BoxIterator
            implements Iterator
        {
            Iterator base;
            Box box;

            public BoxIterator(Iterator base)
            {
                this.base = base;
                this.box = new Box();
            }

            // implement Iterator
            public boolean hasNext()
            {
                return base.hasNext();
            }

            public Object next()
            {
                box.setValue(base.next());
                return box;
            }

            public void remove()
            {
                base.remove();
            }
        }
    }
}
// End CompoundIterator.java
