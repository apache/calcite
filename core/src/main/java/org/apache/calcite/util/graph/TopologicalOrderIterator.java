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

import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Iterates over the edges of a graph in topological order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class TopologicalOrderIterator<V, E extends DefaultEdge>
    implements Iterator<V> {
  final Map<V, int[]> countMap = new HashMap<>();
  final List<V> empties = new ArrayList<>();
  final HepMatchOrder hepMatchOrder;
  private final DefaultDirectedGraph<V, E> graph;

  public TopologicalOrderIterator(DirectedGraph<V, E> graph) {
    this(graph, HepMatchOrder.TOP_DOWN);
  }

  public TopologicalOrderIterator(DirectedGraph<V, E> graph, HepMatchOrder hepMatchOrder) {
    assert hepMatchOrder == HepMatchOrder.TOP_DOWN || hepMatchOrder == HepMatchOrder.BOTTOM_UP;
    this.graph = (DefaultDirectedGraph<V, E>) graph;
    this.hepMatchOrder = hepMatchOrder;
    populate(countMap, empties);
  }

  public static <V, E extends DefaultEdge> Iterable<V> of(
      final DirectedGraph<V, E> graph) {
    return () -> new TopologicalOrderIterator<>(graph);
  }

  public static <V, E extends DefaultEdge> Iterable<V> of(
      final DirectedGraph<V, E> graph, HepMatchOrder hepMatchOrder) {
    return () -> new TopologicalOrderIterator<>(graph, hepMatchOrder);
  }

  @RequiresNonNull("graph")
  private void populate(
      @UnderInitialization TopologicalOrderIterator<V, E> this,
      Map<V, int[]> countMap, List<V> empties) {
    for (V v : graph.vertexMap.keySet()) {
      countMap.put(v, new int[] {0});
    }
    if (hepMatchOrder == HepMatchOrder.TOP_DOWN) {
      for (DefaultDirectedGraph.VertexInfo<V, E> info
          : graph.vertexMap.values()) {
        for (E edge : info.outEdges) {
          //noinspection SuspiciousMethodCalls
          final int[] ints =
              requireNonNull(countMap.get(edge.target),
                  () -> "no value for " + edge.target);
          ++ints[0];
        }
      }
    }
    if (hepMatchOrder == HepMatchOrder.BOTTOM_UP) {
      for (DefaultDirectedGraph.VertexInfo<V, E> info
          : graph.vertexMap.values()) {
        for (E edge : info.outEdges) {
          //noinspection SuspiciousMethodCalls
          final int[] ints =
              requireNonNull(countMap.get(edge.source),
                  () -> "no value for " + edge.source);
          ++ints[0];
        }
      }
    }
    for (Map.Entry<V, int[]> entry : countMap.entrySet()) {
      if (entry.getValue()[0] == 0) {
        empties.add(entry.getKey());
      }
    }
    countMap.keySet().removeAll(empties);
  }

  @Override public boolean hasNext() {
    return !empties.isEmpty();
  }

  @Override public V next() {
    V v = empties.remove(0);
    DefaultDirectedGraph.VertexInfo<V, E> vertexInfo =
        requireNonNull(graph.vertexMap.get(v),
            () -> "no vertex " + v);
    if (hepMatchOrder == HepMatchOrder.TOP_DOWN) {
      for (E o : vertexInfo.outEdges) {
        //noinspection unchecked
        final V target = (V) o.target;
        int[] ints =
            requireNonNull(countMap.get(target),
                () -> "no counts found for target " + target);
        if (--ints[0] == 0) {
          countMap.remove(target);
          empties.add(target);
        }
      }
    }
    if (hepMatchOrder == HepMatchOrder.BOTTOM_UP) {
      for (E o : vertexInfo.inEdges) {
        //noinspection unchecked
        final V source = (V) o.source;
        int[] ints =
            requireNonNull(countMap.get(source),
                () -> "no counts found for source " + source);
        if (--ints[0] == 0) {
          countMap.remove(source);
          empties.add(source);
        }
      }
    }
    return v;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }

  Set<V> findCycles() {
    while (hasNext()) {
      next();
    }
    //noinspection RedundantCast
    return (Set<V>) countMap.keySet();
  }
}
