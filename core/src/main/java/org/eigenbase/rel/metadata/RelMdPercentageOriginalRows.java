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
import org.eigenbase.relopt.*;

import net.hydromatic.optiq.BuiltinMethod;

import com.google.common.collect.ImmutableList;

/**
 * RelMdPercentageOriginalRows supplies a default implementation of
 * {@link RelMetadataQuery#getPercentageOriginalRows} for the standard logical
 * algebra.
 */
public class RelMdPercentageOriginalRows {
  private static final RelMdPercentageOriginalRows INSTANCE =
      new RelMdPercentageOriginalRows();

  public static final RelMetadataProvider SOURCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(
              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltinMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE),

              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltinMethod.CUMULATIVE_COST.method, INSTANCE),

              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltinMethod.NON_CUMULATIVE_COST.method, INSTANCE)));

  //~ Methods ----------------------------------------------------------------

  private RelMdPercentageOriginalRows() {}

  public Double getPercentageOriginalRows(AggregateRelBase rel) {
    // REVIEW jvs 28-Mar-2006: The assumption here seems to be that
    // aggregation does not apply any filtering, so it does not modify the
    // percentage.  That's very much oversimplified.

    return RelMetadataQuery.getPercentageOriginalRows(
        rel.getChild());
  }

  public Double getPercentageOriginalRows(UnionRelBase rel) {
    double numerator = 0.0;
    double denominator = 0.0;

    // Ignore rel.isDistinct() because it's the same as an aggregate.

    // REVIEW jvs 28-Mar-2006: The original Broadbase formula was broken.
    // It was multiplying percentage into the numerator term rather than
    // than dividing it out of the denominator term, which would be OK if
    // there weren't summation going on.  Probably the cause of the error
    // was the desire to avoid division by zero, which I don't know how to
    // handle so I punt, meaning we return a totally wrong answer in the
    // case where a huge table has been completely filtered away.

    for (RelNode input : rel.getInputs()) {
      double rowCount = RelMetadataQuery.getRowCount(input);
      double percentage =
          RelMetadataQuery.getPercentageOriginalRows(input);
      if (percentage != 0.0) {
        denominator += rowCount / percentage;
        numerator += rowCount;
      }
    }

    return quotientForPercentage(numerator, denominator);
  }

  public Double getPercentageOriginalRows(JoinRelBase rel) {
    // Assume any single-table filter conditions have already
    // been pushed down.

    // REVIEW jvs 28-Mar-2006: As with aggregation, this is
    // oversimplified.

    // REVIEW jvs 28-Mar-2006:  need any special casing for SemiJoinRel?

    double left = RelMetadataQuery.getPercentageOriginalRows(rel.getLeft());

    double right =
        RelMetadataQuery.getPercentageOriginalRows(rel.getRight());

    return left * right;
  }

  // Catch-all rule when none of the others apply.
  public Double getPercentageOriginalRows(RelNode rel) {
    if (rel.getInputs().size() > 1) {
      // No generic formula available for multiple inputs.
      return null;
    }

    if (rel.getInputs().size() == 0) {
      // Assume no filtering happening at leaf.
      return 1.0;
    }

    RelNode child = rel.getInputs().get(0);

    Double childPercentage =
        RelMetadataQuery.getPercentageOriginalRows(child);
    if (childPercentage == null) {
      return null;
    }

    // Compute product of percentage filtering from this rel (assuming any
    // filtering is the effect of single-table filters) with the percentage
    // filtering performed by the child.
    Double relPercentage =
        quotientForPercentage(
            RelMetadataQuery.getRowCount(rel),
            RelMetadataQuery.getRowCount(child));
    if (relPercentage == null) {
      return null;
    }
    double percent = relPercentage * childPercentage;

    // this check is needed in cases where this method is called on a
    // physical rel
    if ((percent < 0.0) || (percent > 1.0)) {
      return null;
    }
    return relPercentage * childPercentage;
  }

  // Ditto for getNonCumulativeCost
  public RelOptCost getCumulativeCost(RelNode rel) {
    RelOptCost cost = RelMetadataQuery.getNonCumulativeCost(rel);
    List<RelNode> inputs = rel.getInputs();
    for (RelNode input : inputs) {
      cost = cost.plus(RelMetadataQuery.getCumulativeCost(input));
    }
    return cost;
  }

  // Ditto for getNonCumulativeCost
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    return rel.computeSelfCost(rel.getCluster().getPlanner());
  }

  private static Double quotientForPercentage(
      Double numerator,
      Double denominator) {
    if ((numerator == null) || (denominator == null)) {
      return null;
    }

    // may need epsilon instead
    if (denominator == 0.0) {
      // cap at 100%
      return 1.0;
    } else {
      return numerator / denominator;
    }
  }
}

// End RelMdPercentageOriginalRows.java
