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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;

/**
 * RelMdMaxRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getMaxRowCount} for the standard logical algebra.
 */
public class RelMdMaxRowCount {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.MAX_ROW_COUNT.method, new RelMdMaxRowCount());

  //~ Methods ----------------------------------------------------------------

  public Double getMaxRowCount(Union rel) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = RelMetadataQuery.getMaxRowCount(input);
      if (partialRowCount == null) {
        return null;
      }
      rowCount += partialRowCount;
    }
    return rowCount;
  }

  public Double getMaxRowCount(Intersect rel) {
    // max row count is the smallest of the inputs
    Double rowCount = null;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = RelMetadataQuery.getMaxRowCount(input);
      if (rowCount == null
          || partialRowCount != null && partialRowCount < rowCount) {
        rowCount = partialRowCount;
      }
    }
    return rowCount;
  }

  public Double getMaxRowCount(Minus rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput(0));
  }

  public Double getMaxRowCount(Filter rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(Project rel) {
    return RelMetadataQuery.getMaxRowCount(rel.getInput());
  }

  public Double getMaxRowCount(Sort rel) {
    Double rowCount = RelMetadataQuery.getMaxRowCount(rel.getInput());
    if (rowCount == null) {
      return null;
    }
    final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
    rowCount = Math.max(rowCount - offset, 0D);

    if (rel.fetch != null) {
      final int limit = RexLiteral.intValue(rel.fetch);
      if (limit < rowCount) {
        return (double) limit;
      }
    }
    return rowCount;
  }

  public Double getMaxRowCount(Aggregate rel) {
    if (rel.getGroupSet().isEmpty()) {
      return 1D;
    }
    return RelMetadataQuery.getMaxRowCount(rel.getInput())
        * rel.getGroupSets().size();
  }

  public Double getMaxRowCount(Join rel) {
    Double left = RelMetadataQuery.getMaxRowCount(rel.getLeft());
    Double right = RelMetadataQuery.getMaxRowCount(rel.getRight());
    if (left == null || right == null) {
      return null;
    }
    if (left < 1D && rel.getJoinType().generatesNullsOnLeft()) {
      left = 1D;
    }
    if (right < 1D && rel.getJoinType().generatesNullsOnRight()) {
      right = 1D;
    }
    return left * right;
  }

  public Double getMaxRowCount(TableScan rel) {
    // For typical tables, there is no upper bound to the number of rows.
    return Double.POSITIVE_INFINITY;
  }

  public Double getMaxRowCount(Values values) {
    // For Values, the maximum row count is the actual row count.
    // This is especially useful if Values is empty.
    return (double) values.getTuples().size();
  }

  // Catch-all rule when none of the others apply.
  public Double getMaxRowCount(RelNode rel) {
    return null;
  }
}

// End RelMdMaxRowCount.java
