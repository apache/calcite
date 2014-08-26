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
package net.hydromatic.optiq.util.graph;

import java.util.*;

/**
 * Iterates over the vertices in a directed graph in depth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class DepthFirstIterator<V, E extends DefaultEdge>
    implements Iterator<V> {

  private final Iterator<V> iterator;

  public DepthFirstIterator(DirectedGraph<V, E> graph, V start) {
    // Dumb implementation that builds the list first.
    List<V> list = buildList(graph, start);
    iterator = list.iterator();
  }

  public static <V, E extends DefaultEdge> Iterable<V> of(
      final DirectedGraph<V, E> graph, final V start) {
    return new Iterable<V>() {
      public Iterator<V> iterator() {
        return new DepthFirstIterator<V, E>(graph, start);
      }
    };
  }

  private List<V> buildList(DirectedGraph<V, E> graph, V start) {
    final List<V> list = new ArrayList<V>();
    buildListRecurse(list, new HashSet<V>(), graph, start);
    return list;
  }

  private void buildListRecurse(List<V> list,
      Set<V> activeVertices,
      DirectedGraph<V, E> graph,
      V start) {
    if (!activeVertices.add(start)) {
      return;
    }
    list.add(start);
    List<E> edges = graph.getOutwardEdges(start);
    for (E edge : edges) {
      //noinspection unchecked
      buildListRecurse(list, activeVertices, graph, (V) edge.target);
    }
    activeVertices.remove(start);
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public V next() {
    return iterator.next();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}

// End DepthFirstIterator.java
