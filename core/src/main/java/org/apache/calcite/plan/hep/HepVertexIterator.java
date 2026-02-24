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
package org.apache.calcite.plan.hep;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterates over the vertices in a HepVertex graph in depth-first order.
 * In a HepVertex graph, every HepVertex.getCurrentRel().getInputs() is a
 * List&lt;HepRelVertex&gt;.
 *
 * @param <V> Vertex type
 */
public class HepVertexIterator<V extends HepRelVertex>
    implements Iterator<V> {
  private final Deque<V> deque = new ArrayDeque<>();
  private final Set<Integer> visitedSet;

  private HepVertexIterator(V root, Set<Integer> visitedSet) {
    this.deque.push(root);
    this.visitedSet = visitedSet;
  }

  /**
   * Creates a HepVertexIterator for a given HepVertex root.
   *
   * @param root Root of iteration.
   * @param visitedSet Set of HepVertex IDs to exclude from iteration; next() will add more
   *                   items to it.
   */
  protected static <V extends HepRelVertex> Iterable<V> of(
      final V root, final Set<Integer> visitedSet) {
    return () -> new HepVertexIterator<>(root, visitedSet);
  }

  public Iterator<V> continueFrom(V newVertex) {
    this.deque.push(newVertex);
    return this;
  }

  @Override public boolean hasNext() {
    return !deque.isEmpty();
  }

  @Override public V next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    V v = deque.pop();

    RelNode current = v.getCurrentRel();
    if (current instanceof SingleRel) {
      @SuppressWarnings("unchecked") V target = (V) ((SingleRel) current).getInput();
      if (visitedSet.add(target.getId())) {
        deque.push(target);
      }
    } else {
      for (RelNode input : current.getInputs()) {
        @SuppressWarnings("unchecked") V target = (V) input;
        if (visitedSet.add(target.getId())) {
          deque.push(target);
        }
      }
    }
    return v;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }
}
