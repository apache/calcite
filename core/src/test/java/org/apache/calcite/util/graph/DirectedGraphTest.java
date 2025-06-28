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
import org.apache.calcite.plan.hep.HepMatchOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link DirectedGraph}.
 */
class DirectedGraphTest {
  @Test void testOne() {
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
    assertThat(shortestPath(g, "A", "D"), hasToString("[A, B, C, D]"));
    g.addEdge("B", "D");
    assertThat(shortestPath(g, "A", "D"), hasToString("[A, B, D]"));
    assertThat("There is no path from A to E",
        shortestPath(g, "A", "E"), nullValue());
    assertThat(shortestPath(g, "D", "D"), hasToString("[D]"));
    assertThat("Node X is not in the graph",
        shortestPath(g, "X", "A"), nullValue());
    assertThat(paths(g, "A", "D"), hasToString("[[A, B, D], [A, B, C, D]]"));
  }

  private <V> @Nullable List<V> shortestPath(DirectedGraph<V, DefaultEdge> g,
      V source, V target) {
    List<List<V>> paths = Graphs.makeImmutable(g).getPaths(source, target);
    return paths.isEmpty() ? null : paths.get(0);
  }

  private <V> @Nullable List<V> shortestPath(
      Graphs.FrozenGraph<V, DefaultEdge> g, V source, V target) {
    List<List<V>> paths = g.getPaths(source, target);
    return paths.isEmpty() ? null : paths.get(0);
  }

  private <V> List<List<V>> paths(DirectedGraph<V, DefaultEdge> g,
      V source, V target) {
    return Graphs.makeImmutable(g).getPaths(source, target);
  }

  @Test void testVertexMustExist() {
    DirectedGraph<String, DefaultEdge> g = DefaultDirectedGraph.create();

    final boolean b = g.addVertex("A");
    assertTrue(b);

    final boolean b2 = g.addVertex("A");
    assertFalse(b2);

    try {
      DefaultEdge x = g.addEdge("A", "B");
      fail("expected exception, got " + x);
    } catch (IllegalArgumentException e) {
      // ok
    }
    g.addVertex("B");
    DefaultEdge x = g.addEdge("A", "B");
    assertNotNull(x);
    DefaultEdge x2 = g.addEdge("A", "B");
    assertThat(x2, nullValue());
    try {
      DefaultEdge x3 = g.addEdge("Z", "A");
      fail("expected exception, got " + x3);
    } catch (IllegalArgumentException e) {
      // ok
    }
    g.addVertex("Z");
    DefaultEdge x3 = g.addEdge("Z", "A");
    assertNotNull(x3);
    DefaultEdge x4 = g.addEdge("Z", "A");
    assertThat(x4, nullValue());

    // Attempting to add a vertex already present does not change the graph.
    final List<DefaultEdge> in1 = g.getInwardEdges("A");
    final List<DefaultEdge> out1 = g.getOutwardEdges("A");
    final boolean b3 = g.addVertex("A");
    assertFalse(b3);
    final List<DefaultEdge> in2 = g.getInwardEdges("A");
    final List<DefaultEdge> out2 = g.getOutwardEdges("A");
    assertThat(in1, is(in2));
    assertThat(out1, is(out2));
  }

  /** Unit test for {@link DepthFirstIterator}. */
  @Test void testDepthFirst() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = new ArrayList<>();
    for (String s : DepthFirstIterator.of(graph, "A")) {
      list.add(s);
    }
    assertThat(list, hasToString("[A, B, C, D, E, C, D, F]"));
    list.clear();
    DepthFirstIterator.reachable(list, graph, "A");
    assertThat(list, hasToString("[A, B, C, D, E, C, D, F]"));
  }

  /** Unit test for {@link DepthFirstIterator}. */
  @Test void testPredecessorList() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = Graphs.predecessorListOf(graph, "C");
    assertThat(list, hasToString("[B, E]"));
  }

  /** Unit test for
   * {@link DefaultDirectedGraph#removeAllVertices(java.util.Collection)}. */
  @Test void testRemoveAllVertices() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    assertThat(graph.edgeSet(), hasSize(6));
    graph.removeAllVertices(Arrays.asList("B", "E"));
    assertThat(graph.vertexSet(), hasToString("[A, C, D, F]"));
    assertThat(graph.edgeSet(), hasSize(1));
    assertThat(graph.edgeSet(), hasToString("[C -> D]"));
  }

  /** Unit test for {@link TopologicalOrderIterator}. */
  @Test void testTopologicalOrderIterator() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = new ArrayList<>();
    for (String s : TopologicalOrderIterator.of(graph)) {
      list.add(s);
    }
    assertThat(list, hasToString("[A, B, E, C, F, D]"));
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7030">[CALCITE-7030]
   * Enhance TopologicalOrderIterator to support BOTTOM_UP</a>. */
  @Test void testTopologicalOrderWithBottomUpIterator() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> list = new ArrayList<>();
    for (String s : TopologicalOrderIterator.of(graph, HepMatchOrder.BOTTOM_UP)) {
      list.add(s);
    }
    assertThat(list, hasToString("[D, F, C, B, E, A]"));
  }

  private DefaultDirectedGraph<String, DefaultEdge> createDag() {
    //    D         F
    //    ^         ^
    //    |         |
    //    C <------ +
    //    ^         |
    //    |         |
    //    |         |
    //    B <- A -> E
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

  private DefaultDirectedGraph<String, DefaultEdge> createDag1() {
    //    +--> E <--+
    //    |         |
    //    C         |
    //    ^         D
    //    |         ^
    //    |         |
    //    |         |
    //    B <-- A --+
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
    return graph;
  }

  /** Unit test for
   * {@link org.apache.calcite.util.graph.Graphs.FrozenGraph}. */
  @Test void testPaths() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag1();
    final Graphs.FrozenGraph<String, DefaultEdge> frozenGraph =
        Graphs.makeImmutable(graph);
    assertThat(shortestPath(frozenGraph, "A", "B"), hasToString("[A, B]"));
    assertThat(frozenGraph.getPaths("A", "B"), hasToString("[[A, B]]"));
    assertThat(shortestPath(frozenGraph, "A", "E"), hasToString("[A, D, E]"));
    assertThat(frozenGraph.getPaths("A", "E"),
        hasToString("[[A, D, E], [A, B, C, E]]"));
    assertThat(shortestPath(frozenGraph, "B", "A"), nullValue());

    assertThat(shortestPath(frozenGraph, "D", "C"), nullValue());
    assertThat(frozenGraph.getPaths("D", "E"), hasToString("[[D, E]]"));
    assertThat(shortestPath(frozenGraph, "D", "E"), hasToString("[D, E]"));
  }

  @Test void testDistances() {
    final DefaultDirectedGraph<String, DefaultEdge> graph = createDag1();
    final Graphs.FrozenGraph<String, DefaultEdge> frozenGraph =
        Graphs.makeImmutable(graph);
    assertThat(frozenGraph.getShortestDistance("A", "B"), is(1));
    assertThat(frozenGraph.getShortestDistance("A", "E"), is(2));
    assertThat(frozenGraph.getShortestDistance("B", "A"), is(-1));
    assertThat(frozenGraph.getShortestDistance("D", "C"), is(-1));
    assertThat(frozenGraph.getShortestDistance("D", "E"), is(1));
    assertThat(frozenGraph.getShortestDistance("B", "B"), is(0));
  }

  /** Unit test for {@link org.apache.calcite.util.graph.CycleDetector}. */
  @Test void testCycleDetection() {
    // A - B - C - D
    //  \     /
    //   +- E - F
    DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    assertThat(new CycleDetector<>(graph).findCycles(), empty());

    // Add cycle C-D-E-C
    //
    // A - B - C - D
    //  \     /     \
    //   +- E - F   |
    //      ^      /
    //      \_____/
    graph.addEdge("D", "E");
    assertThat(new CycleDetector<>(graph).findCycles(),
        is(ImmutableSet.of("C", "D", "E", "F")));

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
        is(ImmutableSet.of("C", "D", "E", "F")));

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
        is(ImmutableSet.of("B", "C", "D")));

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
        is(ImmutableSet.of("C", "D")));

    // Empty graph is not cyclic.
    graph.removeAllVertices(graph.vertexSet());
    assertThat(new CycleDetector<>(graph).findCycles(),
        is(ImmutableSet.of()));
  }

  /** Unit test for
   * {@link org.apache.calcite.util.graph.BreadthFirstIterator}. */
  @Test void testBreadthFirstIterator() {
    DefaultDirectedGraph<String, DefaultEdge> graph = createDag();
    final List<String> expected =
        ImmutableList.of("A", "B", "E", "C", "F", "D");
    assertThat(getA(graph, "A"), equalTo(expected));
    assertThat(Lists.newArrayList(getB(graph, "A")), equalTo(expected));
  }

  private List<String> getA(DefaultDirectedGraph<String, DefaultEdge> graph,
      String root) {
    final List<String> list = new ArrayList<>();
    for (String s : BreadthFirstIterator.of(graph, root)) {
      list.add(s);
    }
    return list;
  }

  private Set<String> getB(DefaultDirectedGraph<String, DefaultEdge> graph,
      String root) {
    final Set<String> list = new LinkedHashSet<>();
    BreadthFirstIterator.reachable(list, graph, root);
    return list;
  }

  @Test void testAttributed() {
    AttributedDirectedGraph<String, DefaultEdge> g =
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
    assertThat(shortestPath(g, "A", "D"), hasToString("[A, B, C, D]"));
    g.addEdge("B", "D", 1);
    assertThat(shortestPath(g, "A", "D"), hasToString("[A, B, D]"));
    assertThat("There is no path from A to E", shortestPath(g, "A", "E"),
        nullValue());
    assertThat(shortestPath(g, "D", "D"), hasToString("[D]"));
    assertThat("Node X is not in the graph", shortestPath(g, "X", "A"),
        nullValue());
    assertThat(paths(g, "A", "D"), hasToString("[[A, B, D], [A, B, C, D]]"));
    assertThat(g.addVertex("B"), is(false));

    assertThat(g.getEdges("A", "B"), iterableWithSize(1));
    assertThat(g.addEdge("A", "B", 1), nullValue());
    assertThat(g.getEdges("A", "B"), iterableWithSize(1));
    assertThat(g.addEdge("A", "B", 2), notNullValue());
    assertThat(g.getEdges("A", "B"), iterableWithSize(2));
  }

  @Test void testToString() {
    DefaultDirectedGraph<String, DefaultEdge> g = createDag();
    assertThat(g,
        hasToString("graph(vertices: [A, B, C, D, E, F], "
            + "edges: [A -> B, A -> E, B -> C, C -> D, E -> C, E -> F])"));

    DefaultDirectedGraph<String, DefaultEdge> g1 = createDag1();
    assertThat(g1,
        hasToString("graph(vertices: [A, B, C, D, E, F], "
            + "edges: [A -> B, A -> D, B -> C, C -> E, D -> E])"));
  }

  /** Edge that stores its attributes in a list. */
  private static class DefaultAttributedEdge extends DefaultEdge {
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
          DefaultEdge> {
    public DefaultEdge createEdge(String v0, String v1, Object... attributes) {
      return new DefaultAttributedEdge(v0, v1,
          ImmutableList.copyOf(attributes));
    }

    public DefaultEdge createEdge(String v0, String v1) {
      throw new UnsupportedOperationException();
    }
  }
}
