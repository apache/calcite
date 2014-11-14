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

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.util.BitSets;

/**
 * RelMdSelectivity supplies a default implementation of {@link
 * RelMetadataQuery#getSelectivity} for the standard logical algebra.
 */
public class RelMdSelectivity {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.SELECTIVITY.method, new RelMdSelectivity());

  //~ Constructors -----------------------------------------------------------

  protected RelMdSelectivity() {
  }

  //~ Methods ----------------------------------------------------------------

  public Double getSelectivity(UnionRelBase rel, RexNode predicate) {
    if ((rel.getInputs().size() == 0) || (predicate == null)) {
      return 1.0;
    }

    double sumRows = 0.0;
    double sumSelectedRows = 0.0;
    int[] adjustments = new int[rel.getRowType().getFieldCount()];
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    for (RelNode input : rel.getInputs()) {
      Double nRows = RelMetadataQuery.getRowCount(input);
      if (nRows == null) {
        return null;
      }

      // convert the predicate to reference the types of the union child
      RexNode modifiedPred =
          predicate.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  null,
                  input.getRowType().getFieldList(),
                  adjustments));
      double sel = RelMetadataQuery.getSelectivity(input, modifiedPred);

      sumRows += nRows;
      sumSelectedRows += nRows * sel;
    }

    if (sumRows < 1.0) {
      sumRows = 1.0;
    }
    return sumSelectedRows / sumRows;
  }

  public Double getSelectivity(SortRel rel, RexNode predicate) {
    return RelMetadataQuery.getSelectivity(
        rel.getChild(),
        predicate);
  }

  public Double getSelectivity(FilterRelBase rel, RexNode predicate) {
    // Take the difference between the predicate passed in and the
    // predicate in the filter's condition, so we don't apply the
    // selectivity of the filter twice.  If no predicate is passed in,
    // use the filter's condition.
    if (predicate != null) {
      return RelMetadataQuery.getSelectivity(
          rel.getChild(),
          RelMdUtil.minusPreds(
              rel.getCluster().getRexBuilder(),
              predicate,
              rel.getCondition()));
    } else {
      return RelMetadataQuery.getSelectivity(
          rel.getChild(),
          rel.getCondition());
    }
  }

  public Double getSelectivity(SemiJoinRel rel, RexNode predicate) {
    // create a RexNode representing the selectivity of the
    // semijoin filter and pass it to getSelectivity
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(rel);
    if (predicate != null) {
      newPred =
          rexBuilder.makeCall(
              SqlStdOperatorTable.AND,
              newPred,
              predicate);
    }

    return RelMetadataQuery.getSelectivity(
        rel.getLeft(),
        newPred);
  }

  public Double getSelectivity(AggregateRelBase rel, RexNode predicate) {
    List<RexNode> notPushable = new ArrayList<RexNode>();
    List<RexNode> pushable = new ArrayList<RexNode>();
    RelOptUtil.splitFilters(
        rel.getGroupSet(),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPred =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    Double selectivity =
        RelMetadataQuery.getSelectivity(
            rel.getChild(),
            childPred);
    if (selectivity == null) {
      return null;
    } else {
      RexNode pred =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      return selectivity * RelMdUtil.guessSelectivity(pred);
    }
  }

  public Double getSelectivity(ProjectRelBase rel, RexNode predicate) {
    List<RexNode> notPushable = new ArrayList<RexNode>();
    List<RexNode> pushable = new ArrayList<RexNode>();
    RelOptUtil.splitFilters(
        BitSets.range(rel.getRowType().getFieldCount()),
        predicate,
        pushable,
        notPushable);
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RexNode childPred =
        RexUtil.composeConjunction(rexBuilder, pushable, true);

    RexNode modifiedPred;
    if (childPred == null) {
      modifiedPred = null;
    } else {
      modifiedPred = RelOptUtil.pushFilterPastProject(childPred, rel);
    }
    Double selectivity =
        RelMetadataQuery.getSelectivity(
            rel.getChild(),
            modifiedPred);
    if (selectivity == null) {
      return null;
    } else {
      RexNode pred =
          RexUtil.composeConjunction(rexBuilder, notPushable, true);
      return selectivity * RelMdUtil.guessSelectivity(pred);
    }
  }

  // Catch-all rule when none of the others apply.
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    return RelMdUtil.guessSelectivity(predicate);
  }
}

// End RelMdSelectivity.java
