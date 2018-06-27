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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
  final Set<E> edges = new LinkedHashSet<E>();
  final Map<V, VertexInfo<V, E>> vertexMap =
      new LinkedHashMap<V, VertexInfo<V, E>>();
  final EdgeFactory<V, E> edgeFactory;

  /** Creates a graph. */
  public DefaultDirectedGraph(EdgeFactory<V, E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  public static <V> DefaultDirectedGraph<V, DefaultEdge> create() {
    return create(DefaultEdge.<V>factory());
  }

  public static <V, E extends DefaultEdge> DefaultDirectedGraph<V, E> create(
      EdgeFactory<V, E> edgeFactory) {
    return new DefaultDirectedGraph<V, E>(edgeFactory);
  }

  @Override public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("graph(")
        .append("vertices: ")
        .append(vertexMap.keySet())
        .append(", edges: ")
        .append(edges)
        .append(")");
    return buf.toString();
  }

  public boolean addVertex(V vertex) {
    if (vertexMap.containsKey(vertex)) {
      return false;
    } else {
      vertexMap.put(vertex, new VertexInfo<V, E>());
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
    final VertexInfo<V, E> info2 = vertexMap.get(targetVertex);
    if (info2 == null) {
      throw new IllegalArgumentException("no vertex " + targetVertex);
    }
    final E edge = edgeFactory.createEdge(vertex, targetVertex);
    if (edges.add(edge)) {
      info.outEdges.add(edge);
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
    final VertexInfo<V, E> info = vertexMap.get(source);
    List<E> outEdges = info.outEdges;
    for (int i = 0, size = outEdges.size(); i < size; i++) {
      E edge = outEdges.get(i);
      if (edge.target.equals(target)) {
        outEdges.remove(i);
        edges.remove(edge);
        return true;
      }
    }
    return false;
  }

  public Set<V> vertexSet() {
    return vertexMap.keySet();
  }

  public void removeAllVertices(Collection<V> collection) {
    vertexMap.keySet().removeAll(collection);
    for (VertexInfo<V, E> info : vertexMap.values()) {
      for (Iterator<E> iterator = info.outEdges.iterator();
           iterator.hasNext();) {
        E next = iterator.next();
        //noinspection SuspiciousMethodCalls
        if (collection.contains(next.target)) {
          iterator.remove();
        }
      }
    }
  }

  public List<E> getOutwardEdges(V source) {
    return vertexMap.get(source).outEdges;
  }

  public List<E> getInwardEdges(V target) {
    final ArrayList<E> list = new ArrayList<E>();
    for (VertexInfo<V, E> info : vertexMap.values()) {
      for (E edge : info.outEdges) {
        if (edge.target.equals(target)) {
          list.add(edge);
        }
      }
    }
    return list;
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
   * Information about an edge.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  static class VertexInfo<V, E> {
    public List<E> outEdges = new ArrayList<E>();
  }
}

// End DefaultDirectedGraph.java
