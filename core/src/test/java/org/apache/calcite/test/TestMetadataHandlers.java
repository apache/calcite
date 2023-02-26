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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.Range;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Metadata handlers that are used in {@link MaterializedViewRelOptRulesTest}.
 */
public class TestMetadataHandlers {
  /**
   * Modify the rowCount of the materialized view to be lower than the base table.
   */
  public static class TestRelMdRowCount extends RelMdRowCount {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new TestRelMdRowCount(), BuiltInMetadata.RowCount.Handler.class);

    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
      if (rel.getTable().getQualifiedName().toString().equalsIgnoreCase("[hr, mv_hour]")) {
        // rollup to 24 buckets
        return 24d;
      } else if (rel.getTable().getQualifiedName().toString().equalsIgnoreCase("[hr, mv_minute]")) {
        // rollup to 24*60 buckets
        return 1440d;
      } else if (rel.getTable().getQualifiedName().toString().equalsIgnoreCase("[hr, events]")) {
        // table has one million rows
        return 1000000d;
      } else {
        return 1000d;
      }
    }
  }

  /**
   * Modify the selectivity of SArg to be more selective for a smaller range.
   */
  @SuppressWarnings("BetaApi")
  public static class TestRelMdSelectivity extends RelMdSelectivity {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            new TestRelMdSelectivity(), BuiltInMetadata.Selectivity.Handler.class);

    public Double getSelectivity(RelNode rel, RelMetadataQuery mq,
        @Nullable RexNode predicate) {
      if (predicate == null) {
        return RelMdUtil.guessSelectivity(predicate);
      }
      final RexExecutor executor =
          Util.first(rel.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
      final RexSimplify rexSimplify = new RexSimplify(rel.getCluster().getRexBuilder(),
          RelOptPredicateList.EMPTY, executor);
      RexNode e = rexSimplify.simplify(predicate);
      if (e.getKind() == SqlKind.SEARCH) {
        RexCall call = (RexCall) e;
        Sarg sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
        double selectivity = 0.d;
        for (Object o : sarg.rangeSet.asRanges()) {
          Range r = (Range) o;
          if (!r.hasLowerBound() || !r.hasUpperBound()
              || !(r.lowerEndpoint() instanceof TimestampString)
              || !(r.lowerEndpoint() instanceof TimestampString)) {
            // ignore predicates where the type isn't a timestamp
            return RelMdUtil.guessSelectivity(predicate);
          }
          long lowerBound = ((TimestampString) r.lowerEndpoint()).getMillisSinceEpoch();
          long upperBound = ((TimestampString) r.upperEndpoint()).getMillisSinceEpoch();
          // only used for a range less than one day
          selectivity += (upperBound - lowerBound) / ((double) DateTimeUtils.MILLIS_PER_DAY);
        }
        return selectivity;
      }
      return RelMdUtil.guessSelectivity(predicate);
    }
  }
}
