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

import java.util.Iterator;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link Graph}.
 */
public class GraphTest {
    public GraphTest() {
    }

    @Test public void test() {
        Graph<String> g = new Graph<String>();
        g.createArc("A", "B");
        g.createArc("B", "C");
        g.createArc("D", "C");
        g.createArc("C", "D");
        g.createArc("E", "F");
        g.createArc("C", "C");
        assertEquals(
            "{A-B, B-C, C-D}",
            Graph.Arc.toString(g.getShortestPath("A", "D")));
        g.createArc("B", "D");
        assertEquals(
            "{A-B, B-D}",
            Graph.Arc.toString(g.getShortestPath("A", "D")));
        assertNull(
            "There is no path from A to E",
            g.getShortestPath("A", "E"));
        assertEquals(
            "{}",
            Graph.Arc.toString(g.getShortestPath("D", "D")));
        assertNull(
            "Node X is not in the graph",
            g.getShortestPath("X", "A"));
        assertEquals(
            "{A-B, B-D} {A-B, B-C, C-D}",
            toString(g.getPaths("A", "D")));
    }

    private static <T> String toString(final Iterator<Graph.Arc<T>[]> iter)
    {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        while (iter.hasNext()) {
            Graph.Arc<T>[] path = iter.next();
            if (count++ > 0) {
                buf.append(" ");
            }
            buf.append(Graph.Arc.toString(path));
        }
        return buf.toString();
    }
}

// End GraphTest.java
