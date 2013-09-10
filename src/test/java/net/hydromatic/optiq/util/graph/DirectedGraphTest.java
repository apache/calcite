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
package net.hydromatic.optiq.util.graph;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test for {@link DirectedGraph}.
 */
public class DirectedGraphTest {
  public DirectedGraphTest() {
  }

  @Test public void testOne() {
    DirectedGraph<String, DefaultEdge> g = DefaultDirectedGraph.create();
    g.addVertex("A");
    g.addVertex("B");
    g.addVertex("C");
    g.addVertex("D");
    g.addVertex("E");
    g.addVertex("F");
    g.addEdge("A", "B");
    g.addEdge("B", "C");
    g.addEdge("D", "C");
    g.addEdge("C", "D");
    g.addEdge("E", "F");
    g.addEdge("C", "C");
    assertEquals("[A, B, C, D]", shortestPath(g, "A", "D").toString());
    g.addEdge("B", "D");
    assertEquals("[A, B, D]", shortestPath(g, "A", "D").toString());
    assertNull("There is no path from A to E", shortestPath(g, "A", "E"));
    assertEquals("[]", shortestPath(g, "D", "D").toString());
    assertNull("Node X is not in the graph", shortestPath(g, "X", "A"));
    assertEquals("[[A, B, C, D], [A, B, D]]", paths(g, "A", "D").toString());
  }

  private <V> List<V> shortestPath(DirectedGraph<V, DefaultEdge> g,
      V source, V target) {
    return Graphs.makeImmutable(g).getShortestPath(source, target);
  }

  private <V> List<List<V>> paths(DirectedGraph<V, DefaultEdge> g,
      V source, V target) {
    return Graphs.makeImmutable(g).getPaths(source, target);
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
        DefaultDirectedGraph.create();
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

  /** Unit test for
   * {@link net.hydromatic.optiq.util.graph.Graphs.FrozenGraph}. */
  @Test public void testPaths() {
    //       B -> C
    //      /      \
    //     A        E
    //      \      /
    //       D -->
    final DefaultDirectedGraph<String, DefaultEdge> graph =
        DefaultDirectedGraph.create();
    graph.addVertex("A");
    graph.addVertex("B");
    graph.addVertex("C");
    graph.addVertex("D");
    graph.addVertex("E");
    graph.addVertex("F");
    graph.addEdge("A", "B");
    graph.addEdge("B", "C");
    graph.addEdge("A", "D");
    graph.addEdge("D", "E");
    graph.addEdge("C", "E");
    final Graphs.FrozenGraph<String, DefaultEdge> frozenGraph =
        Graphs.makeImmutable(graph);
    assertEquals("[A, B]", frozenGraph.getShortestPath("A", "B").toString());
    assertEquals("[[A, B]]", frozenGraph.getPaths("A", "B").toString());
    assertEquals("[A, D, E]", frozenGraph.getShortestPath("A", "E").toString());
    assertEquals("[[A, B, C, E], [A, D, E]]",
        frozenGraph.getPaths("A", "E").toString());
    assertNull(frozenGraph.getShortestPath("B", "A"));
    assertNull(frozenGraph.getShortestPath("D", "C"));
    assertEquals("[[D, E]]", frozenGraph.getPaths("D", "E").toString());
    assertEquals("[D, E]", frozenGraph.getShortestPath("D", "E").toString());
  }
}

// End DirectedGraphTest.java
