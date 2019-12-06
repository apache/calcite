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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Relational expression that computes a repeat union (recursive union in SQL
 * terminology).
 *
 * <p>This operation is executed as follows:
 *
 * <ul>
 * <li>Evaluate the left input (i.e., seed relational expression) once.  For
 *   UNION (but not UNION ALL), discard duplicated rows.
 *
 * <li>Evaluate the right input (i.e., iterative relational expression) over and
 *   over until it produces no more results (or until an optional maximum number
 *   of iterations is reached).  For UNION (but not UNION ALL), discard
 *   duplicated results.
 * </ul>
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public abstract class RepeatUnion extends BiRel {

  /**
   * Whether duplicates are considered.
   */
  public final boolean all;

  /**
   * Maximum number of times to repeat the iterative relational expression;
   * negative value means no limit, 0 means only seed will be evaluated
   */
  public final int iterationLimit;

  //~ Constructors -----------------------------------------------------------
  protected RepeatUnion(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode seed, RelNode iterative, boolean all, int iterationLimit) {
    super(cluster, traitSet, seed, iterative);
    this.iterationLimit = iterationLimit;
    this.all = all;
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // TODO implement a more accurate row count?
    double seedRowCount = mq.getRowCount(getSeedRel());
    if (iterationLimit == 0) {
      return seedRowCount;
    }
    return seedRowCount
        + mq.getRowCount(getIterativeRel()) * (iterationLimit < 0 ? 10 : iterationLimit);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (iterationLimit >= 0) {
      pw.item("iterationLimit", iterationLimit);
    }
    return pw.item("all", all);
  }

  public RelNode getSeedRel() {
    return left;
  }

  public RelNode getIterativeRel() {
    return right;
  }

  @Override protected RelDataType deriveRowType() {
    final List<RelDataType> inputRowTypes =
        Lists.transform(getInputs(), RelNode::getRowType);
    final RelDataType rowType =
        getCluster().getTypeFactory().leastRestrictive(inputRowTypes);
    if (rowType == null) {
      throw new IllegalArgumentException("Cannot compute compatible row type "
          + "for arguments: "
          + Util.sepList(inputRowTypes, ", "));
    }
    return rowType;
  }
}

// End RepeatUnion.java
