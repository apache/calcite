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
package org.apache.calcite.rel.hint;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.*;

/**
 * For Rels like aggregate and join, they have groupby keys and join keys, which
 * could has hints. This interface provides a way to specify hints for a list of
 * keys.
 */
@Experimental
public interface KVHintable {
  default RelNode attachHints(List<Pair<Object, List<RelHint>>> hintList) {
    Objects.requireNonNull(hintList);
    final Set<Pair<Object, List<RelHint>>> hints = new LinkedHashSet<>(getHints());
    hints.addAll(hintList);
    return withHints(new ArrayList<>(hints));
  }

  default RelNode withHints(List<Pair<Object, List<RelHint>>> hintList) {
    return (RelNode) this;
  }

  /**
   * Returns the hints of this relational expressions as a list.
   */
  ImmutableList<Pair<Object, List<RelHint>>> getHints();
}

// End KVHintable.java
