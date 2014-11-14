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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util14.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.util.BitSets;

/**
 * RelMdDistinctRowCount supplies a default implementation of {@link
 * RelMetadataQuery#getDistinctRowCount} for the standard logical algebra.
 */
public class RelMdDistinctRowCount {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.DISTINCT_ROW_COUNT.method, new RelMdDistinctRowCount());

  //~ Constructors -----------------------------------------------------------

  protected RelMdDistinctRowCount() {}

  //~ Methods ----------------------------------------------------------------

  public Double getDistinctRowCount(
      UnionRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    Double rowCount = 0.0;
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
          RelMetadataQuery.getDistinctRowCount(
              input,
              groupKey,
              modifiedPred);
      if (partialRowCount == null) {
        return null;
      }
      rowCount += partialRowCount;
    }
    return rowCount;
  }

  public Double getDistinctRowCount(
      SortRel rel,
      BitSet groupKey,
      RexNode predicate) {
    return RelMetadataQuery.getDistinctRowCount(
        rel.getChild(),
        groupKey,
        predicate);
  }

  public Double getDistinctRowCount(
      FilterRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    // REVIEW zfong 4/18/06 - In the Broadbase code, duplicates are not
    // removed from the two filter lists.  However, the code below is
    // doing so.
    RexNode unionPreds =
        RelMdUtil.unionPreds(
            rel.getCluster().getRexBuilder(),
            predicate,
            rel.getCondition());

    return RelMetadataQuery.getDistinctRowCount(
        rel.getChild(),
        groupKey,
        unionPreds);
  }

  public Double getDistinctRowCount(
      JoinRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    return RelMdUtil.getJoinDistinctRowCount(
        rel,
        rel.getJoinType(),
        groupKey,
        predicate,
        false);
  }

  public Double getDistinctRowCount(
      SemiJoinRel rel,
      BitSet groupKey,
      RexNode predicate) {
    // create a RexNode representing the selectivity of the
    // semijoin filter and pass it to getDistinctRowCount
    RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(rel);
    if (predicate != null) {
      RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
      newPred =
          rexBuilder.makeCall(
              SqlStdOperatorTable.AND,
              newPred,
              predicate);
    }

    return RelMetadataQuery.getDistinctRowCount(
        rel.getLeft(),
        groupKey,
        newPred);
  }

  public Double getDistinctRowCount(
      AggregateRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    // determine which predicates can be applied on the child of the
    // aggregate
    List<RexNode> notPushable = new ArrayList<RexNode>();
    List<RexNode> pushable = new ArrayList<RexNode>();
    RelOptUtil.splitFilters(
        rel.getGroupSet(),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPreds =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    // set the bits as they correspond to the child input
    BitSet childKey = new BitSet();
    RelMdUtil.setAggChildKeys(groupKey, rel, childKey);

    Double distinctRowCount =
        RelMetadataQuery.getDistinctRowCount(
            rel.getChild(),
            childKey,
            childPreds);
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

  public Double getDistinctRowCount(
      ValuesRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    Double selectivity = RelMdUtil.guessSelectivity(predicate);

    // assume half the rows are duplicates
    Double nRows = rel.getRows() / 2;
    return RelMdUtil.numDistinctVals(nRows, nRows * selectivity);
  }

  public Double getDistinctRowCount(
      ProjectRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    BitSet baseCols = new BitSet();
    BitSet projCols = new BitSet();
    List<RexNode> projExprs = rel.getProjects();
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

    List<RexNode> notPushable = new ArrayList<RexNode>();
    List<RexNode> pushable = new ArrayList<RexNode>();
    RelOptUtil.splitFilters(
        BitSets.range(rel.getRowType().getFieldCount()),
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
      modifiedPred = RelOptUtil.pushFilterPastProject(childPred, rel);
    }
    Double distinctRowCount =
        RelMetadataQuery.getDistinctRowCount(
            rel.getChild(),
            baseCols,
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
    for (int bit : BitSets.toIter(projCols)) {
      Double subRowCount =
          RelMdUtil.cardOfProjExpr(rel, projExprs.get(bit));
      if (subRowCount == null) {
        return null;
      }
      distinctRowCount *= subRowCount;
    }

    return RelMdUtil.numDistinctVals(
        distinctRowCount,
        RelMetadataQuery.getRowCount(rel));
  }

  // Catch-all rule when none of the others apply.
  public Double getDistinctRowCount(
      RelNode rel,
      BitSet groupKey,
      RexNode predicate) {
    // REVIEW zfong 4/19/06 - Broadbase code does not take into
    // consideration selectivity of predicates passed in.  Also, they
    // assume the rows are unique even if the table is not
    boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(rel, groupKey);
    if (uniq) {
      return NumberUtil.multiply(
          RelMetadataQuery.getRowCount(rel),
          RelMetadataQuery.getSelectivity(rel, predicate));
    }
    return null;
  }
}

// End RelMdDistinctRowCount.java
