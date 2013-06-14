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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;

/**
 * A set of non-negative integers defined by a sequence of points, intervals,
 * and exclusions.
 */
public class IntegerIntervalSet extends AbstractSet<Integer> {
    private final String s;

    private IntegerIntervalSet(String s) {
        this.s = s;
    }

    private static void visit(String s, Handler handler) {
        final String[] split = s.split(",");
        for (String s1 : split) {
            if (s1.isEmpty()) {
                continue;
            }
            boolean exclude = false;
            if (s1.startsWith("-")) {
                s1 = s1.substring(1);
                exclude = true;
            }
            final String[] split1 = s1.split("-");
            if (split1.length == 1) {
                final int n = Integer.parseInt(split1[0]);
                handler.range(n, n, exclude);
            } else {
                final int n0 = Integer.parseInt(split1[0]);
                final int n1 = Integer.parseInt(split1[1]);
                handler.range(n0, n1, exclude);
            }
        }
    }

    /**
     * Parses a range of integers expressed as a string. The string can contain
     * non-negative integers separated by commas, ranges (represented by a
     * hyphen between two integers), and exclusions (represented by a preceding
     * hyphen). For example, "1,2,3-20,-7,-10-15,12".
     *
     * <p>Inclusions and exclusions are performed in the order that they are
     * seen. For example, "1-10,-2-9,3-7,-4-6"</p> does contain 3, because it is
     * included by "1-10", excluded by "-2-9" and last included by "3-7". But it
     * does not include 4.
     *
     * @param s Range set
     */
    public static Set<Integer> of(String s) {
        return new IntegerIntervalSet(s);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Integer> iterator() {
        return Linq4j.enumeratorIterator(enumerator());
    }

    @Override
    public int size() {
        int n = 0;
        Enumerator<Integer> e = enumerator();
        while (e.moveNext()) {
            ++n;
        }
        return n;
    }

    private Enumerator<Integer> enumerator() {
        final int[] bounds = {Integer.MAX_VALUE, Integer.MIN_VALUE};
        visit(
            s,
            new Handler() {
                public void range(int start, int end, boolean exclude) {
                    if (!exclude) {
                        bounds[0] = Math.min(bounds[0], start);
                        bounds[1] = Math.max(bounds[1], end);
                    }
                }
            });
        return new Enumerator<Integer>() {
            int i = bounds[0] - 1;
            public Integer current() {
                return i;
            }

            public boolean moveNext() {
                for (;;) {
                    if (++i > bounds[1]) {
                        return false;
                    }
                    if (contains(i)) {
                        return true;
                    }
                }
            }

            public void reset() {
                i = bounds[0] - 1;
            }
        };
    }

    @Override
    public boolean contains(Object o) {
        return o instanceof Number
            && contains(((Number) o).intValue());
    }

    public boolean contains(final int n) {
        final boolean[] bs = {false};
        visit(
            s,
            new Handler() {
                public void range(int start, int end, boolean exclude) {
                    if (start <= n && n <= end) {
                        bs[0] = !exclude;
                    }
                }
            });
        return bs[0];
    }

    private interface Handler {
        void range(int start, int end, boolean exclude);
    }
}

// End IntegerIntervalSet.java
