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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Miscellaneous graph utilities.
 */
public class Graphs {
  private Graphs() {
  }

  public static <V, E extends TypedEdge<V>> List<V> predecessorListOf(
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
  public static <V, E extends TypedEdge<V>> FrozenGraph<V, E> makeImmutable(
      DirectedGraph<V, E> graph) {
    DefaultDirectedGraph<V, E> graph1 = (DefaultDirectedGraph<V, E>) graph;
    Map<Pair<V, V>, int[]> shortestDistances = new HashMap<>();
    for (DefaultDirectedGraph.VertexInfo<V, E> arc
        : graph1.vertexMap.values()) {
      for (E edge : arc.outEdges) {
        final V source = graph1.source(edge);
        final V target = graph1.target(edge);
        shortestDistances.put(Pair.of(source, target), new int[] {1});
      }
    }
    while (true) {
      // Take a copy of the map's keys to avoid
      // ConcurrentModificationExceptions.
      final List<Pair<V, V>> previous =
          ImmutableList.copyOf(shortestDistances.keySet());
      boolean changed = false;
      for (E edge : graph.edgeSet()) {
        for (Pair<V, V> edge2 : previous) {
          if (edge.target.equals(edge2.left)) {
            final Pair<V, V> key = Pair.of(graph1.source(edge), edge2.right);
            int[] bestDistance = shortestDistances.get(key);
            int[] arc2Distance = shortestDistances.get(edge2);
            if ((bestDistance == null)
                || (bestDistance[0] > (arc2Distance[0] + 1))) {
              shortestDistances.put(key, new int[] {arc2Distance[0] + 1});
              changed = true;
            }
          }
        }
      }
      if (!changed) {
        break;
      }
    }
    return new FrozenGraph<>(graph1, shortestDistances);
  }

  /**
   * Immutable grap.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  public static class FrozenGraph<V, E extends TypedEdge<V>> {
    private final DefaultDirectedGraph<V, E> graph;
    private final Map<Pair<V, V>, int[]> shortestDistances;

    /** Creates a frozen graph as a copy of another graph. */
    FrozenGraph(DefaultDirectedGraph<V, E> graph,
        Map<Pair<V, V>, int[]> shortestDistances) {
      this.graph = graph;
      this.shortestDistances = shortestDistances;
    }

    /**
     * Returns an iterator of all paths between two nodes,
     * in non-decreasing order of path lengths.
     *
     * <p>The current implementation is not optimal.</p>
     */
    public List<List<V>> getPaths(V from, V to) {
      List<List<V>> list = new ArrayList<>();
      if (from.equals(to)) {
        list.add(ImmutableList.of(from));
      }
      findPaths(from, to, list);
      list.sort(Comparator.comparingInt(List::size));
      return list;
    }

    /**
     * Returns the shortest distance between two points, -1, if there is no path.
     * @param from From
     * @param to To
     * @return The shortest distance, -1, if there is no path.
     */
    public int getShortestDistance(V from, V to) {
      if (from.equals(to)) {
        return 0;
      }
      int[] distance = shortestDistances.get(Pair.of(from, to));
      return distance == null ? -1 : distance[0];
    }

    private void findPaths(V from, V to, List<List<V>> list) {
      if (getShortestDistance(from, to) == -1) {
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
