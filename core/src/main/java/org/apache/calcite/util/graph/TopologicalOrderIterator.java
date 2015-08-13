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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Iterates over the edges of a graph in topological order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class TopologicalOrderIterator<V, E extends DefaultEdge>
    implements Iterator<V> {
  final Map<V, int[]> countMap = new HashMap<V, int[]>();
  final List<V> empties = new ArrayList<V>();
  private final DefaultDirectedGraph<V, E> graph;

  public TopologicalOrderIterator(DirectedGraph<V, E> graph) {
    this.graph = (DefaultDirectedGraph<V, E>) graph;
    populate(countMap, empties);
  }

  public static <V, E extends DefaultEdge> Iterable<V> of(
      final DirectedGraph<V, E> graph) {
    return new Iterable<V>() {
      public Iterator<V> iterator() {
        return new TopologicalOrderIterator<V, E>(graph);
      }
    };
  }

  private void populate(Map<V, int[]> countMap, List<V> empties) {
    for (V v : graph.vertexMap.keySet()) {
      countMap.put(v, new int[] {0});
    }
    for (DefaultDirectedGraph.VertexInfo<V, E> info
        : graph.vertexMap.values()) {
      for (E edge : info.outEdges) {
        //noinspection SuspiciousMethodCalls
        final int[] ints = countMap.get(edge.target);
        ++ints[0];
      }
    }
    for (Map.Entry<V, int[]> entry : countMap.entrySet()) {
      if (entry.getValue()[0] == 0) {
        empties.add(entry.getKey());
      }
    }
    countMap.keySet().removeAll(empties);
  }

  public boolean hasNext() {
    return !empties.isEmpty();
  }

  public V next() {
    // The topological ordering will order vertices as u->v
    // if u has more outgoing edges than v.
    // If both have the same number, any order could be picked.
    // The goal is to have an expression evaluated first
    // if there are other expressions dependent on it.
    int chosenIndex = 0;
    int chosenNumOutEdges =
        graph.vertexMap.get(empties.get(chosenIndex)).outEdges.size();
    for (int i = 1; i < empties.size(); ++i) {
      final int numOutEdges =
          graph.vertexMap.get(empties.get(i)).outEdges.size();
      if (chosenNumOutEdges < numOutEdges) {
        chosenIndex = i;
        chosenNumOutEdges = numOutEdges;
      }
    }

    final V v = empties.remove(chosenIndex);
    for (E o : graph.vertexMap.get(v).outEdges) {
      //noinspection unchecked
      final V target = (V) o.target;
      if (--countMap.get(target)[0] == 0) {
        countMap.remove(target);
        empties.add(target);
      }
    }
    return v;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  Set<V> findCycles() {
    while (hasNext()) {
      next();
    }
    return countMap.keySet();
  }
}

// End TopologicalOrderIterator.java
