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
package org.apache.calcite.sql.validate;

import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Filtering requirements of a query, describing "must-filter" fields and
 * "bypass" fields.
 *
 * <p>"Must-filter" fields must be filtered for a query to be considered valid;
 * and "bypass" fields can defuse the errors if they are filtered on as an
 * alternative.
 *
 * <p>Filter requirements originate in a {@link SemanticTable} in the model
 * and propagate to any query that uses that table.
 *
 * <p>For example, consider table {@code t},
 * which has a must-filter field {@code f}
 * and bypass-fields {@code b0} and {@code b1},
 * and the following queries:
 *
 * <ol>
 * <li>Query {@code select f from t}
 * is invalid because there is no filter on {@code f}.
 *
 * <li>Query {@code select * from (select f from t)} gives an error in the
 * subquery because there is no filter on {@code f}.
 *
 * <li>Query {@code select f from t where f = 1} is valid because there is a
 * filter on {@code f}.
 *
 * <li>Query {@code select * from (select f from t) where f = 1} is valid
 * because there is a filter on {@code f}.
 *
 * <li>Query {@code select f from t where b0 = 1} is valid because there is a
 * filter on the bypass-field {@code b0}.
 * </ol>
 *
 * <p>{@code FilterRequirement} is immutable, and has an instance {@link #EMPTY}
 * with no filters.
 *
 * <p><b>Notes on remnantFilterFields</b>
 *
 * <p>{@link #remnantFilterFields} identifies whether the query should error
 * at the top level query. It is populated with the filter-field value when a
 * filter-field is not selected or filtered on, but a bypass-field for the
 * table is selected.
 *
 * <p>A remnant-filter field is no longer accessible by the enclosing query,
 * and so the query can no longer be defused by filtering on it. We must keep
 * track of the remnant-filter field because the query can still be defused by
 * filtering on a bypass-field.
 *
 * <p>For example, consider table {@code t} with a must-filter field {@code f}
 * and bypass-fields {@code b0} and {@code b1}.
 *
 * <ol>
 * <li>Query {@code select b0, b1 from t} results in
 * {@code filterFields} = [],
 * {@code bypassFields} = [{@code b0}, {@code b1}],
 * {@code remnantFilterFields} = [{@code f}].
 * The query is invalid because it is a top-level query and
 * {@link #remnantFilterFields} is not empty.
 *
 * <li>Query {@code select * from (select b0, b1 from t) where b0 = 1} is valid.
 * When unwrapping the subquery we get the same {@code FilterRequirement}
 * as the previous example:
 * {@code filterFields} = [],
 * {@code bypassFields} = [{@code b0}, {@code b1}],
 * {@code remnantFilterFields} = [{@code f}].
 * But when unwrapping the top-level query, the filter on {@code b0} defuses
 * the {@code remnantFilterField} requirement of [{@code f}] because it
 * originated in the same table, resulting in the following:
 * {@code filterFields} = [],
 * {@code bypassFields} = [{@code b0}, {@code b1}],
 * {@code remnantFilterFields} = [].
 * The query is valid because {@link #remnantFilterFields} is now empty.
 * </ol>
 *
 * <p><b>Notes on groups</b>
 *
 * <p>A bypass field only defuses the requirements of the table that it
 * belongs to. When a query reads several tables, for example a join of
 * {@code t} and {@code u}, the filter requirement keeps the requirements of
 * each table instance in a separate {@link Group}, so that a filter on a
 * bypass field of {@code t} cannot defuse the requirements of {@code u}.
 * {@link #filterFields}, {@link #bypassFields} and
 * {@link #remnantFilterFields} are the union of those of the groups.
 *
 * @see SqlValidatorNamespace#getFilterRequirement()
 */
public class FilterRequirement {
  /** Empty filter requirement. */
  public static final FilterRequirement EMPTY =
      new FilterRequirement(ImmutableList.of());

  /** Ordinals (in the row type) of the "must-filter" fields,
   * fields that must be filtered in a query. */
  public final ImmutableBitSet filterFields;

  /** Ordinals (in the row type) of the "bypass" fields,
   * fields that can defuse validation errors on {@link #filterFields}
   * if filtered on. */
  public final ImmutableBitSet bypassFields;

  /** Set of {@link SqlQualified} instances representing fields that have not
   * been defused in the current query, but can still be defused by filtering
   * on a bypass field in the enclosing query. */
  public final ImmutableSet<SqlQualified> remnantFilterFields;

  /** Requirements of each table instance that has not been defused. */
  public final ImmutableList<Group> groups;

  /**
   * Creates a {@code FilterRequirement} with the requirements of a single
   * table instance.
   *
   * @param filterFields Ordinals of the "must-filter" fields
   * @param bypassFields Ordinals of the "bypass" fields
   * @param remnantFilterFields Filter fields that can no longer be filtered on,
   * but can only be defused if a bypass field is filtered on
   */
  FilterRequirement(Iterable<Integer> filterFields,
      Iterable<Integer> bypassFields, Set<SqlQualified> remnantFilterFields) {
    this(
        ImmutableList.of(
            new Group(filterFields, bypassFields, remnantFilterFields)));
  }

  /**
   * Creates a {@code FilterRequirement} from the requirements of several
   * table instances.
   *
   * <p>Groups without any requirement are discarded: their bypass fields
   * have nothing left to defuse.
   *
   * @param groups Requirements of each table instance
   */
  FilterRequirement(Iterable<Group> groups) {
    final ImmutableList.Builder<Group> groupBuilder = ImmutableList.builder();
    final ImmutableBitSet.Builder filterFieldBuilder = ImmutableBitSet.builder();
    final ImmutableBitSet.Builder bypassFieldBuilder = ImmutableBitSet.builder();
    final ImmutableSet.Builder<SqlQualified> remnantFilterFieldBuilder = ImmutableSet.builder();
    for (Group group : groups) {
      if (group.filterFields.isEmpty() && group.remnantFilterFields.isEmpty()) {
        continue;
      }
      groupBuilder.add(group);
      filterFieldBuilder.addAll(group.filterFields);
      bypassFieldBuilder.addAll(group.bypassFields);
      remnantFilterFieldBuilder.addAll(group.remnantFilterFields);
    }
    this.groups = groupBuilder.build();
    this.filterFields = filterFieldBuilder.build();
    this.bypassFields = bypassFieldBuilder.build();
    this.remnantFilterFields = remnantFilterFieldBuilder.build();
  }

  /** Filtering requirements that originate in a single table instance.
   *
   * <p>Filtering on any of the {@link #bypassFields} of a group defuses all
   * of the group's requirements, but not those of other groups. */
  public static class Group {
    /** Ordinals (in the row type) of the "must-filter" fields of the table. */
    public final ImmutableBitSet filterFields;

    /** Ordinals (in the row type) of the "bypass" fields of the table. */
    public final ImmutableBitSet bypassFields;

    /** Must-filter fields of the table that can no longer be filtered on,
     * but can still be defused by filtering on one of the
     * {@link #bypassFields}. */
    public final ImmutableSet<SqlQualified> remnantFilterFields;

    /**
     * Creates a {@code Group}.
     *
     * @param filterFields Ordinals of the "must-filter" fields
     * @param bypassFields Ordinals of the "bypass" fields
     * @param remnantFilterFields Filter fields that can no longer be filtered
     * on, but can only be defused if a bypass field is filtered on
     */
    Group(Iterable<Integer> filterFields, Iterable<Integer> bypassFields,
        Set<SqlQualified> remnantFilterFields) {
      this.filterFields = ImmutableBitSet.of(filterFields);
      this.bypassFields = ImmutableBitSet.of(bypassFields);
      this.remnantFilterFields = ImmutableSet.copyOf(remnantFilterFields);
    }
  }
}
