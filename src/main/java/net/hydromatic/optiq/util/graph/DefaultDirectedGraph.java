/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.util.graph;

import java.util.*;

/**
 * Default implementation of {@link DirectedGraph}.
 */
public class DefaultDirectedGraph<V, E extends DefaultEdge>
    implements DirectedGraph<V, E> {
  final Map<V, VertexInfo<V, E>> vertexMap =
      new LinkedHashMap<V, VertexInfo<V, E>>();
  private final EdgeFactory<V, E> edgeFactory;

  /** Creates a graph. */
  public DefaultDirectedGraph(EdgeFactory<V, E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  public void addVertex(V vertex) {
    vertexMap.put(vertex, new VertexInfo<V, E>());
  }

  public void addEdge(V vertex, V targetVertex) {
    final VertexInfo<V, E> info = vertexMap.get(vertex);
    info.outEdges.add(edgeFactory.createEdge(vertex, targetVertex));
  }

  public boolean removeEdge(V vertex, V targetVertex) {
    final VertexInfo<V, E> info = vertexMap.get(vertex);
    List<E> outEdges = info.outEdges;
    for (int i = 0, size = outEdges.size(); i < size; i++) {
      E outEdge = outEdges.get(i);
      if (outEdge.target.equals(targetVertex)) {
        outEdges.remove(i);
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

  static class VertexInfo<V, E> {
    public List<E> outEdges = new ArrayList<E>();
  }
}

// End DefaultDirectedGraph.java
