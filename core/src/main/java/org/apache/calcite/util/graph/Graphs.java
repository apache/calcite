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

import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.util.Static.cons;

/**
 * Miscellaneous graph utilities.
 */
public class Graphs {
  private Graphs() {
  }

  public static <V, E extends DefaultEdge> List<V> predecessorListOf(
      DirectedGraph<V, E> graph, V vertex) {
    final List<E> edges = graph.getInwardEdges(vertex);
    return new AbstractList<V>() {
      public V get(int index) {
        //noinspection unchecked
        return (V) edges.get(index).source;
      }

      public int size() {
        return edges.size();
      }
    };
  }

  /** Returns a map of the shortest paths between any pair of nodes. */
  public static <V, E extends DefaultEdge> FrozenGraph<V, E> makeImmutable(
      DirectedGraph<V, E> graph) {
    DefaultDirectedGraph<V, E> graph1 = (DefaultDirectedGraph<V, E>) graph;
    Map<Pair<V, V>, List<V>> shortestPaths = new HashMap<>();
    for (DefaultDirectedGraph.VertexInfo<V, E> arc
        : graph1.vertexMap.values()) {
      for (E edge : arc.outEdges) {
        final V source = graph1.source(edge);
        final V target = graph1.target(edge);
        shortestPaths.put(Pair.of(source, target),
            ImmutableList.of(source, target));
      }
    }
    while (true) {
      // Take a copy of the map's keys to avoid
      // ConcurrentModificationExceptions.
      final List<Pair<V, V>> previous =
          ImmutableList.copyOf(shortestPaths.keySet());
      int changeCount = 0;
      for (E edge : graph.edgeSet()) {
        for (Pair<V, V> edge2 : previous) {
          if (edge.target.equals(edge2.left)) {
            final Pair<V, V> key = Pair.of(graph1.source(edge), edge2.right);
            List<V> bestPath = shortestPaths.get(key);
            List<V> arc2Path = shortestPaths.get(edge2);
            if ((bestPath == null)
                || (bestPath.size() > (arc2Path.size() + 1))) {
              shortestPaths.put(key,
                  cons(graph1.source(edge), arc2Path));
              changeCount++;
            }
          }
        }
      }
      if (changeCount == 0) {
        break;
      }
    }
    return new FrozenGraph<>(graph1, shortestPaths);
  }

  /**
   * Immutable grap.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  public static class FrozenGraph<V, E extends DefaultEdge> {
    private final DefaultDirectedGraph<V, E> graph;
    private final Map<Pair<V, V>, List<V>> shortestPaths;

    /** Creates a frozen graph as a copy of another graph. */
    FrozenGraph(DefaultDirectedGraph<V, E> graph,
        Map<Pair<V, V>, List<V>> shortestPaths) {
      this.graph = graph;
      this.shortestPaths = shortestPaths;
    }

    /**
     * Returns an iterator of all paths between two nodes, shortest first.
     *
     * <p>The current implementation is not optimal.</p>
     */
    public List<List<V>> getPaths(V from, V to) {
      List<List<V>> list = new ArrayList<>();
      findPaths(from, to, list);
      return list;
    }

    /**
     * Returns the shortest path between two points, null if there is no path.
     *
     * @param from From
     * @param to To
     *
     * @return A list of arcs, null if there is no path.
     */
    public List<V> getShortestPath(V from, V to) {
      if (from.equals(to)) {
        return ImmutableList.of();
      }
      return shortestPaths.get(Pair.of(from, to));
    }

    private void findPaths(V from, V to, List<List<V>> list) {
      final List<V> shortestPath = shortestPaths.get(Pair.of(from, to));
      if (shortestPath == null) {
        return;
      }
//      final E edge = graph.getEdge(from, to);
//      if (edge != null) {
//        list.add(ImmutableList.of(from, to));
//      }
      final List<V> prefix = new ArrayList<>();
      prefix.add(from);
      findPathsExcluding(from, to, list, new HashSet<>(), prefix);
    }

    /**
     * Finds all paths from "from" to "to" of length 2 or greater, such that the
     * intermediate nodes are not contained in "excludedNodes".
     */
    private void findPathsExcluding(V from, V to, List<List<V>> list,
        Set<V> excludedNodes, List<V> prefix) {
      excludedNodes.add(from);
      for (E edge : graph.edges) {
        if (edge.source.equals(from)) {
          final V target = graph.target(edge);
          if (target.equals(to)) {
            // We found a path.
            prefix.add(target);
            list.add(ImmutableList.copyOf(prefix));
            prefix.remove(prefix.size() - 1);
          } else if (excludedNodes.contains(target)) {
            // ignore it
          } else {
            prefix.add(target);
            findPathsExcluding(target, to, list, excludedNodes, prefix);
            prefix.remove(prefix.size() - 1);
          }
        }
      }
      excludedNodes.remove(from);
    }
  }
}

// End Graphs.java
