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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * RelMdDistinctRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getDistinctRowCount} for the standard logical
 * algebra.
 */
public class RelMdDistinctRowCount
    implements MetadataHandler<BuiltInMetadata.DistinctRowCount> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, new RelMdDistinctRowCount());

  //~ Constructors -----------------------------------------------------------

  protected RelMdDistinctRowCount() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.DistinctRowCount> getDef() {
    return BuiltInMetadata.DistinctRowCount.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(ImmutableBitSet, RexNode)},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getDistinctRowCount(RelNode, ImmutableBitSet, RexNode)
   */
  public Double getDistinctRowCount(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    // REVIEW zfong 4/19/06 - Broadbase code does not take into
    // consideration selectivity of predicates passed in.  Also, they
    // assume the rows are unique even if the table is not
    boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey);
    if (uniq) {
      return NumberUtil.multiply(mq.getRowCount(rel),
          mq.getSelectivity(rel, predicate));
    }
    return null;
  }

  public Double getDistinctRowCount(Union rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    double rowCount = 0.0;
    int[] adjustments = new int[rel.getRowType().getFieldCount()];
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    for (RelNode input : rel.getInputs()) {
      // convert the predicate to reference the types of the union child
      RexNode modifiedPred;
      if (predicate == null) {
        modifiedPred = null;
      } else {
        modifiedPred =
            predicate.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    null,
                    input.getRowType().getFieldList(),
                    adjustments));
      }
      Double partialRowCount =
          mq.getDistinctRowCount(input, groupKey, modifiedPred);
      if (partialRowCount == null) {
        return null;
      }
      rowCount += partialRowCount;
    }
    return rowCount;
  }

  public Double getDistinctRowCount(Sort rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate);
  }

  public Double getDistinctRowCount(Exchange rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    return mq.getDistinctRowCount(rel.getInput(), groupKey, predicate);
  }

  public Double getDistinctRowCount(Filter rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }
    // REVIEW zfong 4/18/06 - In the Broadbase code, duplicates are not
    // removed from the two filter lists.  However, the code below is
    // doing so.
    RexNode unionPreds =
        RelMdUtil.unionPreds(
            rel.getCluster().getRexBuilder(),
            predicate,
            rel.getCondition());

    return mq.getDistinctRowCount(rel.getInput(), groupKey, unionPreds);
  }

  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    return RelMdUtil.getJoinDistinctRowCount(mq, rel, rel.getJoinType(),
        groupKey, predicate, false);
  }

  public Double getDistinctRowCount(Aggregate rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }
    // determine which predicates can be applied on the child of the
    // aggregate
    final List<RexNode> notPushable = new ArrayList<>();
    final List<RexNode> pushable = new ArrayList<>();
    RelOptUtil.splitFilters(
        rel.getGroupSet(),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPreds =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    // set the bits as they correspond to the child input
    ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
    RelMdUtil.setAggChildKeys(groupKey, rel, childKey);

    Double distinctRowCount =
        mq.getDistinctRowCount(rel.getInput(), childKey.build(), childPreds);
    if (distinctRowCount == null) {
      return null;
    } else if (notPushable.isEmpty()) {
      return distinctRowCount;
    } else {
      RexNode preds =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      return distinctRowCount * RelMdUtil.guessSelectivity(preds);
    }
  }

  public Double getDistinctRowCount(Values rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }
    double selectivity = RelMdUtil.guessSelectivity(predicate);

    // assume half the rows are duplicates
    double nRows = rel.estimateRowCount(mq) / 2;
    return RelMdUtil.numDistinctVals(nRows, nRows * selectivity);
  }

  public Double getDistinctRowCount(Project rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }
    ImmutableBitSet.Builder baseCols = ImmutableBitSet.builder();
    ImmutableBitSet.Builder projCols = ImmutableBitSet.builder();
    List<RexNode> projExprs = rel.getProjects();
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

    final List<RexNode> notPushable = new ArrayList<>();
    final List<RexNode> pushable = new ArrayList<>();
    RelOptUtil.splitFilters(
        ImmutableBitSet.range(rel.getRowType().getFieldCount()),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // get the distinct row count of the child input, passing in the
    // columns and filters that only reference the child; convert the
    // filter to reference the children projection expressions
    RexNode childPred =
        RexUtil.composeConjunction(rexBuilder, pushable, true);
    RexNode modifiedPred;
    if (childPred == null) {
      modifiedPred = null;
    } else {
      modifiedPred = RelOptUtil.pushPastProject(childPred, rel);
    }
    Double distinctRowCount =
        mq.getDistinctRowCount(rel.getInput(), baseCols.build(),
            modifiedPred);

    if (distinctRowCount == null) {
      return null;
    } else if (!notPushable.isEmpty()) {
      RexNode preds =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      distinctRowCount *= RelMdUtil.guessSelectivity(preds);
    }

    // No further computation required if the projection expressions
    // are all column references
    if (projCols.cardinality() == 0) {
      return distinctRowCount;
    }

    // multiply by the cardinality of the non-child projection expressions
    for (int bit : projCols.build()) {
      Double subRowCount =
          RelMdUtil.cardOfProjExpr(mq, rel, projExprs.get(bit));
      if (subRowCount == null) {
        return null;
      }
      distinctRowCount *= subRowCount;
    }

    return RelMdUtil.numDistinctVals(distinctRowCount, mq.getRowCount(rel));
  }

  public Double getDistinctRowCount(RelSubset rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    final RelNode best = rel.getBest();
    if (best != null) {
      return mq.getDistinctRowCount(best, groupKey, predicate);
    }
    if (!Bug.CALCITE_1048_FIXED) {
      return getDistinctRowCount((RelNode) rel, mq, groupKey, predicate);
    }
    Double d = null;
    for (RelNode r2 : rel.getRels()) {
      try {
        Double d2 = mq.getDistinctRowCount(r2, groupKey, predicate);
        d = NumberUtil.min(d, d2);
      } catch (CyclicMetadataException e) {
        // Ignore this relational expression; there will be non-cyclic ones
        // in this set.
      }
    }
    return d;
  }
}

// End RelMdDistinctRowCount.java
