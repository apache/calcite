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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Expression combined with sort flags (DESCENDING, NULLS LAST).
 */
public class RexFieldCollation extends Pair<RexNode, ImmutableSet<SqlKind>> {
  public RexFieldCollation(RexNode left, Set<SqlKind> right) {
    super(left, ImmutableSet.copyOf(right));
  }

  @Override public String toString() {
    String s = left.toString();
    for (SqlKind operator : right) {
      switch (operator) {
      case DESCENDING:
        s += " DESC";
        break;
      case NULLS_FIRST:
        s += " NULLS FIRST";
        break;
      case NULLS_LAST:
        s += " NULLS LAST";
        break;
      default:
        throw new AssertionError(operator);
      }
    }
    return s;
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
            : RelFieldCollation.NullDirection.UNSPECIFIED;
  }
}

// End RexFieldCollation.java
