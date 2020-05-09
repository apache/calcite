/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.util.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link DirectedGraph}.
 */
class DirectedGraphTest {
  @Test void testOne() {
    DirectedGraph<String, TypedEdge<String>> g = DefaultDirectedGraph.create();
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
    assertNull(shortestPath(g, "A", "E"), "There is no path from A to E");
    assertEquals("[D]", shortestPath(g, "D", "D").toString());
    assertNull(shortestPath(g, "X", "A"), "Node X is not in the graph");
    assertEquals("[[A, B, D], [A, B, C, D]]", paths(g, "A", "D").toString());
  }

  private <V> List<V> shortestPath(DirectedGraph<V, TypedEdge<V>> g,
      V source, V target) {
    List<List<V>> paths = Graphs.makeImmutable(g).getPaths(source, target);
    return paths.isEmpty() ? null : paths.get(0);
  }

  private <V> List<V> shortestPath(Graphs.FrozenGraph<V, TypedEdge<V>> g,
                                   V source, V target) {
    List<List<V>> paths = g.getPaths(source, target);
    return paths.isEmpty() ? null : paths.get(0);
  }

  private <V> List<List<V>> paths(DirectedGraph<V, TypedEdge<V>> g,
      V source, V target) {
    return Graphs.makeImmutable(g).getPaths(source, target);
  }

  @Test void testVertexMustExist() {
    DirectedGraph<String, TypedEdge<String>> g = DefaultDirectedGraph.create();

    final boolean b = g.addVertex("A");
    assertTrue(b);

    final boolean b2 = g.addVertex("A");
    assertFalse(b2);

    try {
      TypedEdge<String> x = g.addEdge("A", "B");
      fail("expected exception, got " + x);
    } catch (IllegalArgumentException e) {
      // ok
    }
    g.addVertex("B");
    TypedEdge<String> x = g.addEdge("A", "B");
    assertNotNull(x);
    TypedEdge<String> x2 = g.addEdge("A", "B");
    assertNull(x2);
    try {
      TypedEdge<String> x3 = g.addEdge("Z", "A");
      fail("expected exception, got " + x3);
    } catch (IllegalArgumentException e) {
      // ok
    }
    g.addVertex("Z");
    TypedEdge<String> x3 = g.addEdge("Z", "A");
    assertNotNull(x3);
    TypedEdge<String> x4 = g.addEdge("Z", "A");
    assertNull(x4);

    // Attempting to add a vertex already present does not change the graph.
    final List<TypedEdge<String>> in1 = g.getInwardEdges("A");
    final List<TypedEdge<String>> out1 = g.getOutwardEdges("A");
    final boolean b3 = g.addVertex("A");
    assertFalse(b3);
    final List<TypedEdge<String>> in2 = g.getInwardEdges("A");
    final List<TypedEdge<String>> out2 = g.getOutwardEdges("A");
    assertEquals(in1, in2);
    assertEquals(out1, out2);
  }

  /** Unit test for {@link DepthFirstIterator}. */
  @Test void testDepthFirst() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    final List<String> list = new ArrayList<String>();
    for (String s : DepthFirstIterator.of(graph, "A")) {
      list.add(s);
    }
    assertThat(list.toString(), equalTo("[A, B, C, D, E, C, D, F]"));
    list.clear();
    DepthFirstIterator.reachable(list, graph, "A");
    assertThat(list.toString(), equalTo("[A, B, C, D, E, C, D, F]"));
  }

  /** Unit test for {@link DepthFirstIterator}. */
  @Test void testPredecessorList() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    final List<String> list = Graphs.predecessorListOf(graph, "C");
    assertEquals("[B, E]", list.toString());
  }

  /** Unit test for
   * {@link DefaultDirectedGraph#removeAllVertices(java.util.Collection)}. */
  @Test void testRemoveAllVertices() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    graph.removeAllVertices(Arrays.asList("B", "E"));
    assertEquals("[A, C, D, F]", graph.vertexSet().toString());
  }

  /** Unit test for {@link TopologicalOrderIterator}. */
  @Test void testTopologicalOrderIterator() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    final List<String> list = new ArrayList<String>();
    for (String s : TopologicalOrderIterator.of(graph)) {
      list.add(s);
    }
    assertEquals("[A, B, E, C, F, D]", list.toString());
  }

  private DefaultDirectedGraph<String, TypedEdge<String>> createDag() {
    //    D         F
    //    ^         ^
    //    |         |
    //    C <------ +
    //    ^         |
    //    |         |
    //    |         |
    //    B <- A -> E
    final DefaultDirectedGraph<String, TypedEdge<String>> graph =
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

  private DefaultDirectedGraph<String, TypedEdge<String>> createDag1() {
    //    +--> E <--+
    //    |         |
    //    C         |
    //    ^         D
    //    |         ^
    //    |         |
    //    |         |
    //    B <-- A --+
    final DefaultDirectedGraph<String, TypedEdge<String>> graph =
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
    return graph;
  }

  /** Unit test for
   * {@link org.apache.calcite.util.graph.Graphs.FrozenGraph}. */
  @Test void testPaths() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag1();
    final Graphs.FrozenGraph<String, TypedEdge<String>> frozenGraph =
        Graphs.makeImmutable(graph);
    assertEquals("[A, B]", shortestPath(frozenGraph, "A", "B").toString());
    assertEquals("[[A, B]]", frozenGraph.getPaths("A", "B").toString());
    assertEquals("[A, D, E]", shortestPath(frozenGraph, "A", "E").toString());
    assertEquals("[[A, D, E], [A, B, C, E]]",
        frozenGraph.getPaths("A", "E").toString());
    assertNull(shortestPath(frozenGraph, "B", "A"));

    assertNull(shortestPath(frozenGraph, "D", "C"));
    assertEquals("[[D, E]]", frozenGraph.getPaths("D", "E").toString());
    assertEquals("[D, E]", shortestPath(frozenGraph, "D", "E").toString());
  }

  @Test void testDistances() {
    final DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag1();
    final Graphs.FrozenGraph<String, TypedEdge<String>> frozenGraph =
        Graphs.makeImmutable(graph);
    assertEquals(1, frozenGraph.getShortestDistance("A", "B"));
    assertEquals(2, frozenGraph.getShortestDistance("A", "E"));
    assertEquals(-1, frozenGraph.getShortestDistance("B", "A"));
    assertEquals(-1, frozenGraph.getShortestDistance("D", "C"));
    assertEquals(1, frozenGraph.getShortestDistance("D", "E"));
    assertEquals(0, frozenGraph.getShortestDistance("B", "B"));
  }

  /** Unit test for {@link org.apache.calcite.util.graph.CycleDetector}. */
  @Test void testCycleDetection() {
    // A - B - C - D
    //  \     /
    //   +- E - F
    DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(ImmutableSet.of()));

    // Add cycle C-D-E-C
    //
    // A - B - C - D
    //  \     /     \
    //   +- E - F   |
    //      ^      /
    //      \_____/
    graph.addEdge("D", "E");
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(
            ImmutableSet.of("C", "D", "E", "F")));

    // Add another cycle, D-C-D in addition to C-D-E-C.
    //           __
    //          /  \
    // A - B - C - D
    //  \     /     \
    //   +- E - F   |
    //      ^      /
    //      \_____/
    graph.addEdge("D", "C");
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(
            ImmutableSet.of("C", "D", "E", "F")));

    graph.removeEdge("D", "E");
    graph.removeEdge("D", "C");
    graph.addEdge("C", "B");

    // Add cycle of length 2, C-B-C
    //       __
    //      /  \
    // A - B - C - D
    //  \     /
    //   +- E - F
    //
    // Detected cycle contains "D", which is downstream from the cycle but not
    // in the cycle. Not sure whether that is correct.
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(
            ImmutableSet.of("B", "C", "D")));

    // Add single-node cycle, C-C
    //
    //        ___
    //        \ /
    // A - B - C - D
    //  \     /
    //   +- E - F
    graph.removeEdge("C", "B");
    graph.addEdge("C", "C");
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(
            ImmutableSet.of("C", "D")));

    // Empty graph is not cyclic.
    graph.removeAllVertices(graph.vertexSet());
    assertThat(new CycleDetector<>(graph).findCycles(),
        CoreMatchers.equalTo(ImmutableSet.of()));
  }

  /** Unit test for
   * {@link org.apache.calcite.util.graph.BreadthFirstIterator}. */
  @Test void testBreadthFirstIterator() {
    DefaultDirectedGraph<String, TypedEdge<String>> graph = createDag();
    final List<String> expected =
        ImmutableList.of("A", "B", "E", "C", "F", "D");
    assertThat(getA(graph, "A"), equalTo(expected));
    assertThat(Lists.newArrayList(getB(graph, "A")), equalTo(expected));
  }

  private List<String> getA(DefaultDirectedGraph<String, TypedEdge<String>> graph,
      String root) {
    final List<String> list = new ArrayList<String>();
    for (String s : BreadthFirstIterator.of(graph, root)) {
      list.add(s);
    }
    return list;
  }

  private Set<String> getB(DefaultDirectedGraph<String, TypedEdge<String>> graph,
      String root) {
    final Set<String> list = new LinkedHashSet<String>();
    BreadthFirstIterator.reachable(list, graph, root);
    return list;
  }

  @Test void testAttributed() {
    AttributedDirectedGraph<String, TypedEdge<String>> g =
        AttributedDirectedGraph.create(new DefaultAttributedEdgeFactory());
    g.addVertex("A");
    g.addVertex("B");
    g.addVertex("C");
    g.addVertex("D");
    g.addVertex("E");
    g.addVertex("F");
    g.addEdge("A", "B", 1);
    g.addEdge("B", "C", 1);
    g.addEdge("D", "C", 1);
    g.addEdge("C", "D", 1);
    g.addEdge("E", "F", 1);
    g.addEdge("C", "C", 1);
    assertEquals("[A, B, C, D]", shortestPath(g, "A", "D").toString());
    g.addEdge("B", "D", 1);
    assertEquals("[A, B, D]", shortestPath(g, "A", "D").toString());
    assertNull(shortestPath(g, "A", "E"), "There is no path from A to E");
    assertEquals("[D]", shortestPath(g, "D", "D").toString());
    assertNull(shortestPath(g, "X", "A"), "Node X is not in the graph");
    assertEquals("[[A, B, D], [A, B, C, D]]", paths(g, "A", "D").toString());
    assertThat(g.addVertex("B"), is(false));

    assertThat(Iterables.size(g.getEdges("A", "B")), is(1));
    assertThat(g.addEdge("A", "B", 1), nullValue());
    assertThat(Iterables.size(g.getEdges("A", "B")), is(1));
    assertThat(g.addEdge("A", "B", 2), notNullValue());
    assertThat(Iterables.size(g.getEdges("A", "B")), is(2));
  }

  /** Edge that stores its attributes in a list. */
  private static class DefaultAttributedEdge extends TypedEdge<String> {
    private final List list;

    DefaultAttributedEdge(String source, String target, List list) {
      super(source, target);
      this.list = ImmutableList.copyOf(list);
    }

    @Override public int hashCode() {
      return super.hashCode() * 31 + list.hashCode();
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj instanceof DefaultAttributedEdge
          && ((DefaultAttributedEdge) obj).source.equals(source)
          && ((DefaultAttributedEdge) obj).target.equals(target)
          && ((DefaultAttributedEdge) obj).list.equals(list);
    }
  }

    /** Factory for {@link DefaultAttributedEdge}. */
  private static class DefaultAttributedEdgeFactory
      implements AttributedDirectedGraph.AttributedEdgeFactory<String,
        TypedEdge<String>> {
    public TypedEdge<String> createEdge(String v0, String v1, Object... attributes) {
      return new DefaultAttributedEdge(v0, v1,
          ImmutableList.copyOf(attributes));
    }

    public TypedEdge<String> createEdge(String v0, String v1) {
      throw new UnsupportedOperationException();
    }
  }
}
