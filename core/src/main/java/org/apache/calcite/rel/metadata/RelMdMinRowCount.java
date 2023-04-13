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

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RelMdMinRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getMinRowCount} for the standard logical algebra.
 */
public class RelMdMinRowCount
    implements MetadataHandler<BuiltInMetadata.MinRowCount> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdMinRowCount(), BuiltInMetadata.MinRowCount.Handler.class);

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.MinRowCount> getDef() {
    return BuiltInMetadata.MinRowCount.DEF;
  }

  public Double getMinRowCount(Union rel, RelMetadataQuery mq) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double partialRowCount = mq.getMinRowCount(input);
      if (partialRowCount != null) {
        rowCount += partialRowCount;
      }
    }

    if (rel.all) {
      return rowCount;
    } else {
      return Math.min(rowCount, 1d);
    }
  }

  public Double getMinRowCount(Intersect rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public Double getMinRowCount(Minus rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public Double getMinRowCount(Filter rel, RelMetadataQuery mq) {
    return 0d; // no lower bound
  }

  public @Nullable Double getMinRowCount(Calc rel, RelMetadataQuery mq) {
    if (rel.getProgram().getCondition() != null) {
      // no lower bound
      return 0d;
    } else {
      return mq.getMinRowCount(rel.getInput());
    }
  }

  public @Nullable Double getMinRowCount(Project rel, RelMetadataQuery mq) {
    return mq.getMinRowCount(rel.getInput());
  }

  public @Nullable Double getMinRowCount(Exchange rel, RelMetadataQuery mq) {
    return mq.getMinRowCount(rel.getInput());
  }

  public @Nullable Double getMinRowCount(TableModify rel, RelMetadataQuery mq) {
    return mq.getMinRowCount(rel.getInput());
  }

  public Double getMinRowCount(Sort rel, RelMetadataQuery mq) {
    Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount == null) {
      rowCount = 0D;
    }

    final int offset = rel.offset instanceof RexLiteral ? RexLiteral.intValue(rel.offset) : 0;
    rowCount = Math.max(rowCount - offset, 0D);

    final double limit =
        rel.fetch instanceof RexLiteral ? RexLiteral.intValue(rel.fetch) : rowCount;
    return limit < rowCount ? limit : rowCount;
  }

  public Double getMinRowCount(EnumerableLimit rel, RelMetadataQuery mq) {
    Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount == null) {
      rowCount = 0D;
    }

    final int offset = rel.offset instanceof RexLiteral ? RexLiteral.intValue(rel.offset) : 0;
    rowCount = Math.max(rowCount - offset, 0D);

    final double limit =
        rel.fetch instanceof RexLiteral ? RexLiteral.intValue(rel.fetch) : rowCount;
    return limit < rowCount ? limit : rowCount;
  }

  public Double getMinRowCount(Aggregate rel, RelMetadataQuery mq) {
    if (rel.getGroupSet().isEmpty()) {
      // Aggregate with no GROUP BY always returns 1 row (even on empty table).
      return 1D;
    }
    final Double rowCount = mq.getMinRowCount(rel.getInput());
    if (rowCount != null && rowCount >= 1D) {
      return (double) rel.getGroupSets().size();
    }
    return 0D;
  }

  public Double getMinRowCount(Join rel, RelMetadataQuery mq) {
    return 0D;
  }

  public @Nullable Double getMinRowCount(TableScan rel, RelMetadataQuery mq) {
    final BuiltInMetadata.MinRowCount.Handler handler =
        rel.getTable().unwrap(BuiltInMetadata.MinRowCount.Handler.class);
    if (handler != null) {
      return handler.getMinRowCount(rel, mq);
    }
    return 0D;
  }

  public Double getMinRowCount(Values values, RelMetadataQuery mq) {
    // For Values, the minimum row count is the actual row count.
    return (double) values.getTuples().size();
  }

  public Double getMinRowCount(RelSubset rel, RelMetadataQuery mq) {
    // FIXME This is a short-term fix for [CALCITE-1018]. A complete
    // solution will come with [CALCITE-1048].
    Util.discard(Bug.CALCITE_1048_FIXED);
    for (RelNode node : rel.getRels()) {
      if (node instanceof Sort) {
        Sort sort = (Sort) node;
        if (sort.fetch instanceof RexLiteral) {
          return (double) RexLiteral.intValue(sort.fetch);
        }
      }
    }

    return 0D;
  }

  // Catch-all rule when none of the others apply.
  public @Nullable Double getMinRowCount(RelNode rel, RelMetadataQuery mq) {
    return null;
  }
}
