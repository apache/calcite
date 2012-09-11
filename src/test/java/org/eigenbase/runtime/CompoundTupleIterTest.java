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

import java.util.ArrayList;

import org.eigenbase.test.EigenbaseTestCase;

/**
 * Unit test for {@link CompoundTupleIter}.
 *
 * @author jhyde
 */
public class CompoundTupleIterTest
    extends EigenbaseTestCase
{
    public CompoundTupleIterTest(String s)
        throws Exception
    {
        super(s);
    }

    public void testCompoundIter()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] { "a", "b" }),
                    makeTupleIter(new String[] { "c" })
                });
        assertEquals(
            iterator,
            new String[] { "a", "b", "c" });
    }

    public void testCompoundIterEmpty()
    {
        TupleIter iterator = new CompoundTupleIter(new TupleIter[] {});
        assertEquals(
            iterator,
            new String[] {});
    }

    public void testCompoundIterFirstEmpty()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    makeTupleIter(new String[] {}),
                    makeTupleIter(new String[] { "a", null }),
                    makeTupleIter(new String[] {}),
                    makeTupleIter(new String[] {}),
                    makeTupleIter(new String[] { "b", "c" }),
                    makeTupleIter(new String[] {})
                });
        assertEquals(
            iterator,
            new String[] { "a", null, "b", "c" });
    }

    // makes a trivial CalcTupleIter on top of a base TupleIter
    private static CalcTupleIter makeCalcTupleIter(final TupleIter base)
    {
        return new CalcTupleIter(base) {
            public Object fetchNext()
            {
                return base.fetchNext();
            }
        };
    }

    public void testCompoundCalcIter()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    makeCalcTupleIter(
                        makeTupleIter(new String[] { "a", "b" })),
                    makeCalcTupleIter(makeTupleIter(new String[] { "c" }))
                });
        assertEquals(
            iterator,
            new String[] { "a", "b", "c" });
    }

    public void testCompoundCalcIterFirstEmpty()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    makeCalcTupleIter(makeTupleIter(new String[] {})),
                    makeCalcTupleIter(makeTupleIter(new String[] { "a" })),
                    makeCalcTupleIter(makeTupleIter(new String[] {})),
                    makeCalcTupleIter(makeTupleIter(new String[] {})),
                    makeCalcTupleIter(
                        makeTupleIter(new String[] { "b", "c" })),
                    makeCalcTupleIter(makeTupleIter(new String[] {}))
                });
        assertEquals(
            iterator,
            new String[] { "a", "b", "c" });
    }

    /**
     * Checks that a BoxTupleIter returns the same values as the contents of
     * an array.
     */
    protected void assertUnboxedEquals(TupleIter p, Object [] a)
    {
        ArrayList<Object> list = new ArrayList<Object>();
        while (true) {
            Object o = p.fetchNext();
            if (o instanceof Box) {
                list.add(((Box) o).getValue());
            } else if (o == TupleIter.NoDataReason.END_OF_DATA) {
                break;
            } else {
                list.add(o);
            }
        }
        assertEquals(list, a);
    }

    public void testCompoundBoxIter()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    new BoxTupleIter(
                        makeTupleIter(
                            new String[] { "400", "401", "402", "403" })),
                    new BoxTupleIter(
                        makeTupleIter(
                            new String[] { "500", "501", "502", "503" })),
                    new BoxTupleIter(
                        makeTupleIter(
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

    public void testCompoundBoxedCalcIter()
    {
        TupleIter iterator =
            new CompoundTupleIter(
                new TupleIter[] {
                    new BoxTupleIter(
                        makeCalcTupleIter(
                            makeTupleIter(
                                new String[] {
                                    "400", "401", "402", "403"
                                }))),
                    new BoxTupleIter(
                        makeCalcTupleIter(
                            makeTupleIter(
                                new String[] {
                                    "500", "501", "502", "503"
                                }))),
                    new BoxTupleIter(
                        makeCalcTupleIter(
                            makeTupleIter(
                                new String[] {
                                    "600", "601", "602", "603"
                                })))
                });
        assertUnboxedEquals(
            iterator,
            new String[] {
                "400", "401", "402", "403",
                "500", "501", "502", "503",
                "600", "601", "602", "603"
            });
    }

    // a boxed value (see BoxTupleIter below)
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

    // An TupleIter that always returns the same object, a Box, but with
    // different contents. Mimics the TupleIter from a farrago dynamic
    // statement.
    static class BoxTupleIter
        extends AbstractTupleIter
    {
        TupleIter base;
        Box box;

        public BoxTupleIter(TupleIter base)
        {
            this.base = base;
            this.box = new Box();
        }

        // implement TupleIter
        public Object fetchNext()
        {
            Object result = base.fetchNext();
            if (result instanceof NoDataReason) {
                return result;
            }

            box.setValue(result);
            return box;
        }

        // implement TupleIter
        public void restart()
        {
            throw new UnsupportedOperationException();
        }

        // implement TupleIter
        public void closeAllocation()
        {
            box = null;
        }
    }
}

// End CompoundTupleIterTest.java
