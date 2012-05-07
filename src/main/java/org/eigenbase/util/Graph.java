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

import junit.framework.*;


/**
 * A <code>Graph</code> is a collection of directed arcs between nodes, and
 * supports various graph-theoretic operations.
 *
 * @author jhyde
 * @version $Id$
 * @since May 6, 2003
 */
public class Graph<T>
{
    //~ Static fields/initializers ---------------------------------------------

    public static final Arc [] noArcs = new Arc[0];

    //~ Instance fields --------------------------------------------------------

    /**
     * Maps {@link Arc} to {@link Arc}[].
     */
    private Map<Arc<T>, Arc<T>[]> shortestPath =
        new HashMap<Arc<T>, Arc<T>[]>();
    private Set<Arc<T>> arcs = new HashSet<Arc<T>>();
    private boolean mutable = true;

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns an iterator of all paths between two nodes, shortest first.
     *
     * <p>The current implementation is not optimal.</p>
     */
    public Iterator<Arc<T>[]> getPaths(
        T from,
        T to)
    {
        List<Arc<T>[]> list = new ArrayList<Arc<T>[]>();
        findPaths(from, to, list);
        return list.iterator();
    }

    /**
     * Returns the shortest path between two points, null if there is no path.
     *
     * @param from
     * @param to
     *
     * @return A list of arcs, null if there is no path.
     */
    public Arc<T> [] getShortestPath(
        T from,
        T to)
    {
        if (from.equals(to)) {
            return noArcs;
        }
        makeImmutable();
        return shortestPath.get(new Arc<T>(from, to));
    }

    public Arc createArc(
        T from,
        T to)
    {
        final Arc<T> arc = new Arc<T>(from, to);
        arcs.add(arc);
        mutable = true;
        return arc;
    }

    /**
     * Removes an arc between two vertices.
     *
     * @return The arc removed, or null
     */
    public Arc deleteArc(
        T from,
        T to)
    {
        final Arc arc = new Arc<T>(from, to);
        if (arcs.remove(arc)) {
            mutable = true;
            return arc;
        } else {
            return null;
        }
    }

    private void findPaths(
        T from,
        T to,
        List<Arc<T>[]> list)
    {
        final Arc [] shortestPath = getShortestPath(from, to);
        if (shortestPath == null) {
            return;
        }
        Arc<T> arc = new Arc<T>(from, to);
        if (arcs.contains(arc)) {
            list.add(new Arc[] { arc });
        }
        findPathsExcluding(
            from,
            to,
            list,
            new HashSet<T>(),
            new ArrayList<Arc<T>>());
    }

    /**
     * Finds all paths from "from" to "to" of length 2 or greater, such that the
     * intermediate nodes are not contained in "excludedNodes".
     */
    private void findPathsExcluding(
        T from,
        T to,
        List<Arc<T>[]> list,
        Set<T> excludedNodes,
        List<Arc<T>> prefix)
    {
        excludedNodes.add(from);
        for (Arc<T> arc : arcs) {
            if (arc.from.equals(from)) {
                if (arc.to.equals(to)) {
                    // We found a path.
                    prefix.add(arc);
                    final Arc<T> [] arcs = prefix.toArray(noArcs);
                    list.add(arcs);
                    prefix.remove(prefix.size() - 1);
                } else if (excludedNodes.contains(arc.to)) {
                    // ignore it
                } else {
                    prefix.add(arc);
                    findPathsExcluding(arc.to, to, list, excludedNodes, prefix);
                    prefix.remove(prefix.size() - 1);
                }
            }
        }
        excludedNodes.remove(from);
    }

    private void makeImmutable()
    {
        if (mutable) {
            mutable = false;
            shortestPath.clear();
            for (Arc<T> arc : arcs) {
                shortestPath.put(
                    arc,
                    new Arc[] { arc });
            }
            while (true) {
                // Take a copy of the map's keys to avoid
                // ConcurrentModificationExceptions.
                ArrayList<Arc> previous =
                    new ArrayList<Arc>(shortestPath.keySet());
                int changeCount = 0;
                for (Arc<T> arc : arcs) {
                    for (Arc<T> arc2 : previous) {
                        if (arc.to.equals(arc2.from)) {
                            final Arc<T> newArc = new Arc<T>(arc.from, arc2.to);
                            Arc [] bestPath = shortestPath.get(newArc);
                            Arc [] arc2Path = shortestPath.get(arc2);
                            if ((bestPath == null)
                                || (bestPath.length > (arc2Path.length + 1)))
                            {
                                Arc<T> [] newPath =
                                    new Arc[arc2Path.length + 1];
                                newPath[0] = arc;
                                System.arraycopy(
                                    arc2Path,
                                    0,
                                    newPath,
                                    1,
                                    arc2Path.length);
                                shortestPath.put(newArc, newPath);
                                changeCount++;
                            }
                        }
                    }
                }
                if (changeCount == 0) {
                    break;
                }
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * An <code>Arc</code> is a directed link between two nodes.
     *
     * <p>The nodes are compared according to {@link Object#equals} and {@link
     * Object#hashCode}. We assume that their {@link Object#toString} works,
     * too.</p>
     */
    public static class Arc<T>
    {
        public final T from;
        public final T to;
        private final String string; // for debug

        /**
         * Creates an arc.
         *
         * @pre from != null
         * @pre to != null
         */
        public Arc(
            T from,
            T to)
        {
            this.from = from;
            this.to = to;
            this.string = toString();
            Util.discard(this.string);
        }

        public boolean equals(Object obj)
        {
            if (obj instanceof Arc) {
                Arc other = (Arc) obj;
                return from.equals(other.from) && to.equals(other.to);
            }
            return false;
        }

        public int hashCode()
        {
            return from.hashCode() ^ (to.hashCode() << 4);
        }

        public String toString()
        {
            return from + "-" + to;
        }

        private static <T> String toString(Arc<T> [] arcs)
        {
            StringBuilder buf = new StringBuilder("{");
            for (int i = 0; i < arcs.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(arcs[i].toString());
            }
            buf.append("}");
            return buf.toString();
        }
    }

    public static class GraphTest
        extends TestCase
    {
        public GraphTest(String name)
        {
            super(name);
        }

        public void test()
        {
            Graph<String> g = new Graph<String>();
            g.createArc("A", "B");
            g.createArc("B", "C");
            g.createArc("D", "C");
            g.createArc("C", "D");
            g.createArc("E", "F");
            g.createArc("C", "C");
            assertEquals(
                "{A-B, B-C, C-D}",
                Arc.toString(g.getShortestPath("A", "D")));
            g.createArc("B", "D");
            assertEquals(
                "{A-B, B-D}",
                Arc.toString(g.getShortestPath("A", "D")));
            assertNull(
                "There is no path from A to E",
                g.getShortestPath("A", "E"));
            assertEquals(
                "{}",
                Arc.toString(g.getShortestPath("D", "D")));
            assertNull(
                "Node X is not in the graph",
                g.getShortestPath("X", "A"));
            assertEquals(
                "{A-B, B-D} {A-B, B-C, C-D}",
                toString(g.getPaths("A", "D")));
        }

        private static <T> String toString(final Iterator<Arc<T>[]> iter)
        {
            StringBuilder buf = new StringBuilder();
            int count = 0;
            while (iter.hasNext()) {
                Arc<T> [] path = iter.next();
                if (count++ > 0) {
                    buf.append(" ");
                }
                buf.append(Arc.toString(path));
            }
            return buf.toString();
        }
    }
}

// End Graph.java
