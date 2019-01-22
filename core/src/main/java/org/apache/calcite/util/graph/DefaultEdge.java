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

import java.util.Objects;

/**
 * Default implementation of Edge.
 */
public class DefaultEdge {
  public final Object source;
  public final Object target;

  public DefaultEdge(Object source, Object target) {
    this.source = Objects.requireNonNull(source);
    this.target = Objects.requireNonNull(target);
  }

  @Override public int hashCode() {
    return source.hashCode() * 31 + target.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof DefaultEdge
        && ((DefaultEdge) obj).source.equals(source)
        && ((DefaultEdge) obj).target.equals(target);
  }

  public static <V> DirectedGraph.EdgeFactory<V, DefaultEdge> factory() {
    return DefaultEdge::new;
  }
}

// End DefaultEdge.java
