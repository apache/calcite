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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for relational expressions with {@link RelHint}s.
 *
 * <p>Relational expressions that can attach hints should implement
 * this interface.
 *
 * <p>This interface is experimental, currently, we make
 * {@link org.apache.calcite.rel.core.Project}
 * {@link org.apache.calcite.rel.core.Join}
 * {@link org.apache.calcite.rel.core.TableScan}
 * implement this interface and add an argument named "hints" to construct these
 * relational expressions if hints are attached.
 *
 * <p>This design is not that elegant and mature, because we have to copy the hints whenever these
 * relational expressions are copied or used to derive new relational expressions.
 * Even though we have implemented the mechanism to propagate the hints, for large queries,
 * there would be many cases where the hints are not copied to the right RelNode,
 * and the effort/memory is wasted if we are copying the hint to a RelNode
 * but the hint is not used.
 */
@Experimental
public interface Hintable {

  /**
   * Attach list of hints to this relational expression, should be overridden by
   * every logical node that supports hint. This method is only for
   * internal use during sql-to-rel conversion.
   *
   * <p>The sub-class should return a new copy of the relational expression. We make
   * the default implementation return the relational expression directly only
   * because not every kind of relational expression supports hints.
   *
   * @param hintList The hints to attach to this relational expression
   * @return Relational expression with the hints {@code hintList} attached
   */
  default RelNode attachHints(List<RelHint> hintList) {
    return (RelNode) this;
  }

  /**
   * @return The hints list of this relational expressions
   */
  ImmutableList<RelHint> getHints();

  /**
   * Merge this relation expression's hints with the given hint list.
   *
   * <p>The default behavior is to put them in one list and eliminate the duplicates.
   *
   * @param hintList Hints to be merged
   * @return A merged hint list
   */
  default List<RelHint> mergeHints(List<RelHint> hintList) {
    Objects.requireNonNull(hintList);
    final Set<RelHint> hints = new LinkedHashSet<>(getHints());
    hints.addAll(hintList);
    return new ArrayList<>(hints);
  }
}

// End Hintable.java
