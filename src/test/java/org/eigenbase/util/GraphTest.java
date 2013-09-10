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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.hydromatic.optiq.util.graph.*;
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

  /** Unit test for {@link DepthFirstIterator}. */
  @Test public void testDepthFirst() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = new ArrayList<String>();
    for (String s : DepthFirstIterator.of(graph, "A")) {
      list.add(s);
    }
    assertEquals("[A, B, C, D, E, C, D, F]", list.toString());
  }

  /** Unit test for {@link DepthFirstIterator}. */
  @Test public void testPredecessorList() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = Graphs.predecessorListOf(graph, "C");
    assertEquals("[B, E]", list.toString());
  }

  /** Unit test for
   * {@link DefaultDirectedGraph#removeAllVertices(java.util.Collection)}. */
  @Test public void testRemoveAllVertices() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    graph.removeAllVertices(Arrays.asList("B", "E"));
    assertEquals("[A, C, D, F]", graph.vertexSet().toString());
  }

  /** Unit test for {@link TopologicalOrderIterator}. */
  @Test public void testTopologicalOrderIterator() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = new ArrayList<String>();
    for (String s : TopologicalOrderIterator.of(graph)) {
      list.add(s);
    }
    assertEquals("[A, B, E, C, F, D]", list.toString());
  }

  private DefaultDirectedGraph<String, DefaultEdge> createDag() {
    final DefaultDirectedGraph<String, DefaultEdge> graph =
        new DefaultDirectedGraph<String, DefaultEdge>(
            DefaultEdge.<String>factory());
    graph.addVertex("A");
    graph.addVertex("B");
    graph.addVertex("C");
    graph.addVertex("D");
    graph.addVertex("E");
    graph.addVertex("F");
    graph.addEdge("A", "B");
    graph.addEdge("B", "C");
    graph.addEdge("C", "D");
    graph.addEdge("A", "E");
    graph.addEdge("E", "C");
    graph.addEdge("E", "F");
    return graph;
  }
}

// End GraphTest.java
