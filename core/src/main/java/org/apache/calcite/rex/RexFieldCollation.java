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
package org.apache.calcite.rex;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Expression combined with sort flags (DESCENDING, NULLS LAST).
 */
public class RexFieldCollation extends Pair<RexNode, ImmutableSet<SqlKind>> {
  /** Canonical map of all combinations of {@link SqlKind} values that can ever
   * occur. We use a canonical map to save a bit of memory. Because the sets
   * are EnumSets they have predictable order for toString(). */
  private static final ImmutableMap<Set<SqlKind>, ImmutableSet<SqlKind>> KINDS =
      new Initializer()
          .add()
          .add(SqlKind.NULLS_FIRST)
          .add(SqlKind.NULLS_LAST)
          .add(SqlKind.DESCENDING)
          .add(SqlKind.DESCENDING, SqlKind.NULLS_FIRST)
          .add(SqlKind.DESCENDING, SqlKind.NULLS_LAST)
          .build();

  public RexFieldCollation(RexNode left, Set<SqlKind> right) {
    super(left, requireNonNull(KINDS.get(right)));
  }

  @Override public String toString() {
    final String s = left.toString();
    if (right.isEmpty()) {
      return s;
    }
    final StringBuilder b = new StringBuilder(s);
    for (SqlKind operator : right) {
      switch (operator) {
      case DESCENDING:
        b.append(" DESC");
        break;
      case NULLS_FIRST:
        b.append(" NULLS FIRST");
        break;
      case NULLS_LAST:
        b.append(" NULLS LAST");
        break;
      default:
        throw new AssertionError(operator);
      }
    }
    return b.toString();
  }

  public RelFieldCollation.Direction getDirection() {
    return right.contains(SqlKind.DESCENDING)
        ? RelFieldCollation.Direction.DESCENDING
        : RelFieldCollation.Direction.ASCENDING;
  }

  public RelFieldCollation.NullDirection getNullDirection() {
    return right.contains(SqlKind.NULLS_LAST)
        ? RelFieldCollation.NullDirection.LAST
        : right.contains(SqlKind.NULLS_FIRST)
            ? RelFieldCollation.NullDirection.FIRST
            : getDirection().defaultNullDirection();
  }

  /** Helper, used during initialization, that builds a canonizing map from
   * sets of {@code SqlKind} to immutable sets of {@code SqlKind}. */
  private static class Initializer {
    final ImmutableMap.Builder<Set<SqlKind>, ImmutableSet<SqlKind>> builder =
        ImmutableMap.builder();

    public Initializer add() {
      return add(ImmutableSet.of());
    }

    public Initializer add(SqlKind kind, SqlKind... kinds) {
      return add(Sets.immutableEnumSet(kind, kinds));
    }

    private Initializer add(ImmutableSet<SqlKind> set) {
      builder.put(set, set);
      return this;
    }

    public ImmutableMap<Set<SqlKind>, ImmutableSet<SqlKind>> build() {
      return builder.build();
    }
  }
}
