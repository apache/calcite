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

import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link DirectedGraph}.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class DefaultDirectedGraph<V, E extends TypedEdge<V>>
    implements DirectedGraph<V, E> {
  final Set<E> edges = new LinkedHashSet<>();
  final Map<V, VertexInfo<V, E>> vertexMap = new LinkedHashMap<>();
  final EdgeFactory<V, E> edgeFactory;

  /** Creates a graph. */
  public DefaultDirectedGraph(EdgeFactory<V, E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  public static <V> DefaultDirectedGraph<V, TypedEdge<V>> create() {
    return create(TypedEdge.factory());
  }

  public static <V, E extends TypedEdge<V>> DefaultDirectedGraph<V, E> create(
      EdgeFactory<V, E> edgeFactory) {
    return new DefaultDirectedGraph<>(edgeFactory);
  }

  public String toStringUnordered() {
    return "graph("
        + "vertices: " + vertexMap.keySet()
        + ", edges: " + edges + ")";
  }

  @Override public String toString() {
    @SuppressWarnings("unchecked")
    final Ordering<V> vertexOrdering = (Ordering) Ordering.usingToString();
    @SuppressWarnings("unchecked")
    final Ordering<E> edgeOrdering = (Ordering) Ordering.usingToString();
    return toString(vertexOrdering, edgeOrdering);
  }

  /** Returns the string representation of this graph, using the given
   * orderings to ensure that the output order of vertices and edges is
   * deterministic. */
  private String toString(Ordering<V> vertexOrdering,
      Ordering<E> edgeOrdering) {
    return "graph("
        + "vertices: " + vertexOrdering.sortedCopy(vertexMap.keySet())
        + ", edges: " + edgeOrdering.sortedCopy(edges) + ")";
  }

  public boolean addVertex(V vertex) {
    if (vertexMap.containsKey(vertex)) {
      return false;
    } else {
      vertexMap.put(vertex, new VertexInfo<>());
      return true;
    }
  }

  public Set<E> edgeSet() {
    return Collections.unmodifiableSet(edges);
  }

  public E addEdge(V vertex, V targetVertex) {
    final VertexInfo<V, E> info = vertexMap.get(vertex);
    if (info == null) {
      throw new IllegalArgumentException("no vertex " + vertex);
    }
    final VertexInfo<V, E> targetInfo = vertexMap.get(targetVertex);
    if (targetInfo == null) {
      throw new IllegalArgumentException("no vertex " + targetVertex);
    }
    final E edge = edgeFactory.createEdge(vertex, targetVertex);
    if (edges.add(edge)) {
      info.outEdges.add(edge);
      targetInfo.inEdges.add(edge);
      return edge;
    } else {
      return null;
    }
  }

  public E getEdge(V source, V target) {
    // REVIEW: could instead use edges.get(new DefaultEdge(source, target))
    final VertexInfo<V, E> info = vertexMap.get(source);
    for (E outEdge : info.outEdges) {
      if (outEdge.target.equals(target)) {
        return outEdge;
      }
    }
    return null;
  }

  public boolean removeEdge(V source, V target) {
    // remove out edges
    final List<E> outEdges = vertexMap.get(source).outEdges;
    boolean outRemoved = false;
    for (int i = 0, size = outEdges.size(); i < size; i++) {
      E edge = outEdges.get(i);
      if (edge.target.equals(target)) {
        outEdges.remove(i);
        edges.remove(edge);
        outRemoved = true;
        break;
      }
    }

    // remove in edges
    final List<E> inEdges = vertexMap.get(target).inEdges;
    boolean inRemoved = false;
    for (int i = 0, size = inEdges.size(); i < size; i++) {
      E edge = inEdges.get(i);
      if (edge.source.equals(source)) {
        inEdges.remove(i);
        inRemoved = true;
        break;
      }
    }
    assert outRemoved == inRemoved;
    return outRemoved;
  }

  public Set<V> vertexSet() {
    return vertexMap.keySet();
  }

  public void removeAllVertices(Collection<V> collection) {
    // The point at which collection is large enough to make the 'majority'
    // algorithm more efficient.
    final float threshold = 0.35f;
    final int thresholdSize = (int) (vertexMap.size() * threshold);
    if (collection.size() > thresholdSize && !(collection instanceof Set)) {
      // Convert collection to a set, so that collection.contains() is
      // faster. If there are duplicates, collection.size() will get smaller.
      collection = new HashSet<>(collection);
    }
    if (collection.size() > thresholdSize) {
      removeMajorityVertices((Set<V>) collection);
    } else {
      removeMinorityVertices(collection);
    }
  }

  /** Implementation of {@link #removeAllVertices(Collection)} that is efficient
   * if {@code collection} is a small fraction of the set of vertices. */
  private void removeMinorityVertices(Collection<V> collection) {
    for (V v : collection) {
      final VertexInfo<V, E> info = vertexMap.get(v);
      if (info == null) {
        continue;
      }

      // remove all edges pointing to v
      for (E edge : info.inEdges) {
        final V source = edge.source;
        final VertexInfo<V, E> sourceInfo = vertexMap.get(source);
        sourceInfo.outEdges.removeIf(e -> e.target.equals(v));
      }

      // remove all edges starting from v
      for (E edge : info.outEdges) {
        final V target = edge.target;
        final VertexInfo<V, E> targetInfo = vertexMap.get(target);
        targetInfo.inEdges.removeIf(e -> e.source.equals(v));
      }
    }
    vertexMap.keySet().removeAll(collection);
  }

  /** Implementation of {@link #removeAllVertices(Collection)} that is efficient
   * if {@code vertexSet} is a large fraction of the set of vertices in the
   * graph. */
  private void removeMajorityVertices(Set<V> vertexSet) {
    vertexMap.keySet().removeAll(vertexSet);
    for (VertexInfo<V, E> info : vertexMap.values()) {
      info.outEdges.removeIf(e -> vertexSet.contains(e.target));
      info.inEdges.removeIf(e -> vertexSet.contains(e.source));
    }
  }

  public List<E> getOutwardEdges(V source) {
    return vertexMap.get(source).outEdges;
  }

  public List<E> getInwardEdges(V target) {
    return vertexMap.get(target).inEdges;
  }

  final V source(E edge) {
    //noinspection unchecked
    return edge.source;
  }

  final V target(E edge) {
    //noinspection unchecked
    return edge.target;
  }

  /**
   * Information about a vertex.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  static class VertexInfo<V, E> {
    final List<E> outEdges = new ArrayList<>();
    final List<E> inEdges = new ArrayList<>();
  }
}
