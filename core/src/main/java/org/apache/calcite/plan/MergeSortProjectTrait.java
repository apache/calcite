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
package org.apache.calcite.plan;

import com.google.common.collect.ImmutableList;

import java.util.Collection;

/**
 * Marks a {@link org.apache.calcite.rel.core.Sort} whose input {@link
 * org.apache.calcite.rel.core.Project} can be merged into the enclosing SELECT because some of
 * its columns exist only to give the sort collation something to reference.
 *
 * <p>A Calcite {@code Sort} collation can only address input fields by ordinal, so
 * {@code RelBuilder.sortLimit} materialises any non-{@code RexInputRef} sort key as an extra
 * projected column, then re-projects the original row type on top. Those sort-only columns are
 * not part of the user's SELECT list and must not be printed.
 *
 * <p>The trait records their field indexes in the Sort's input row type so that
 * {@link org.apache.calcite.rel.rel2sql.SqlImplementor} can (a) skip wrapping the query in a
 * sub-query merely because they are unprojected, and (b) render the ORDER BY item as the
 * underlying expression instead of an ordinal or alias that will no longer resolve.
 *
 * <p>The producer must only attach this trait when the sort expression refers exclusively to
 * columns of the FROM clause, since inlining it into ORDER BY re-evaluates it in that scope.
 */
public class MergeSortProjectTrait implements RelTrait {

  private final ImmutableList<Integer> sortOnlyFieldIndexes;

  public MergeSortProjectTrait(Collection<Integer> sortOnlyFieldIndexes) {
    this.sortOnlyFieldIndexes = ImmutableList.copyOf(sortOnlyFieldIndexes);
  }

  public ImmutableList<Integer> getSortOnlyFieldIndexes() {
    return sortOnlyFieldIndexes;
  }

  public boolean isSortOnlyField(int index) {
    return sortOnlyFieldIndexes.contains(index);
  }

  @Override public RelTraitDef<MergeSortProjectTrait> getTraitDef() {
    return MergeSortProjectTraitDef.instance;
  }

  @Override public boolean satisfies(RelTrait trait) {
    return this.equals(trait);
  }

  @Override public void register(RelOptPlanner planner) {
  }

  @Override public String toString() {
    return "MergeSortProject" + sortOnlyFieldIndexes;
  }
}
