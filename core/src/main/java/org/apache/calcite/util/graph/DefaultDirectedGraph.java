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
public class DefaultDirectedGraph<V, E extends DefaultEdge>
    implements DirectedGraph<V, E> {
  final Set<E> edges = new LinkedHashSet<>();

  /**
   * The map for out-edges.
   */
  final Map<V, VertexInfo<V, E>> vertexOutMap = new LinkedHashMap<>();

  /**
   * The map for in-edges.
   */
  final Map<V, VertexInfo<V, E>> vertexInMap = new LinkedHashMap<>();

  final EdgeFactory<V, E> edgeFactory;

  /** Creates a graph. */
  public DefaultDirectedGraph(EdgeFactory<V, E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  public static <V> DefaultDirectedGraph<V, DefaultEdge> create() {
    return create(DefaultEdge.factory());
  }

  public static <V, E extends DefaultEdge> DefaultDirectedGraph<V, E> create(
      EdgeFactory<V, E> edgeFactory) {
    return new DefaultDirectedGraph<>(edgeFactory);
  }

  public String toStringUnordered() {
    return "graph("
        + "vertices: " + vertexOutMap.keySet()
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
        + "vertices: " + vertexOrdering.sortedCopy(vertexOutMap.keySet())
        + ", edges: " + edgeOrdering.sortedCopy(edges) + ")";
  }

  public boolean addVertex(V vertex) {
    if (vertexOutMap.containsKey(vertex)) {
      return false;
    } else {
      vertexOutMap.put(vertex, new VertexInfo<>());
      vertexInMap.put(vertex, new VertexInfo<>());
      return true;
    }
  }

  public Set<E> edgeSet() {
    return Collections.unmodifiableSet(edges);
  }

  public E addEdge(V vertex, V targetVertex) {
    final VertexInfo<V, E> outInfo = vertexOutMap.get(vertex);
    if (outInfo == null) {
      throw new IllegalArgumentException("no vertex " + vertex);
    }
    final VertexInfo<V, E> inInfo = vertexInMap.get(targetVertex);
    if (inInfo == null) {
      throw new IllegalArgumentException("no vertex " + targetVertex);
    }
    final E edge = edgeFactory.createEdge(vertex, targetVertex);
    if (edges.add(edge)) {
      outInfo.edges.add(edge);
      inInfo.edges.add(edge);
      return edge;
    } else {
      return null;
    }
  }

  public E getEdge(V source, V target) {
    // REVIEW: could instead use edges.get(new DefaultEdge(source, target))
    final VertexInfo<V, E> info = vertexOutMap.get(source);
    for (E outEdge : info.edges) {
      if (outEdge.target.equals(target)) {
        return outEdge;
      }
    }
    return null;
  }

  public boolean removeEdge(V source, V target) {
    // remove out edges
    final VertexInfo<V, E> outInfo = vertexOutMap.get(source);
    List<E> outEdges = outInfo.edges;
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
    final VertexInfo<V, E> inInfo = vertexInMap.get(target);
    List<E> inEdges = inInfo.edges;
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
    return vertexOutMap.keySet();
  }

  public void removeAllVertices(Collection<V> collection) {
    // remove out edges
    vertexOutMap.keySet().removeAll(collection);
    for (VertexInfo<V, E> info : vertexOutMap.values()) {
      //noinspection SuspiciousMethodCalls
      info.edges.removeIf(next -> collection.contains(next.target));
    }

    // remove in edges
    vertexInMap.keySet().removeAll(collection);
    for (VertexInfo<V, E> info : vertexInMap.values()) {
      //noinspection SuspiciousMethodCalls
      info.edges.removeIf(next -> collection.contains(next.source));
    }
  }

  public List<E> getOutwardEdges(V source) {
    return vertexOutMap.get(source).edges;
  }

  public List<E> getInwardEdges(V target) {
    return vertexInMap.get(target).edges;
  }

  final V source(E edge) {
    //noinspection unchecked
    return (V) edge.source;
  }

  final V target(E edge) {
    //noinspection unchecked
    return (V) edge.target;
  }

  /**
   * Information about a vertex.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  static class VertexInfo<V, E> {
    public List<E> edges = new ArrayList<>();
  }
}
