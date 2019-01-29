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
 * Relational expression that computes a Repeat Union (a.k.a. Recursive Union in SQL terminology).
 *
 * The structure of a Recursive Union is always: non-recursive term + UNION [ALL] + recursive term,
 * where only the recursive term can contain a reference to the query's own output.
 * (TODO only UNION ALL is supported for the moment)
 *
 * Such an operation is executed as follows:
 *   - Evaluate the non-recursive term once. For UNION (but not UNION ALL), discard duplicate
 *   results.
 *   - (Re)Evaluate the recursive term over and over until it produces no more results. For UNION
 *   (but not UNION ALL), discard duplicate results.
 * Note: strictly speaking, this process is iteration not recursion, but RECURSIVE is the
 * terminology chosen by the SQL standards committee.
 *
 * In this RepeatUnion relational expression, the left part will represent the non-recursive
 * term (or "seed" relational expression), and the right part will represent the recursive term
 * (or "iterative" relational expression).
 *
 * <p>NOTE: The current API is experimental and subject to change without notice.</p>
 */
@Experimental
public abstract class RepeatUnion extends BiRel {

  protected final boolean all = true; // TODO for the moment only RepeatUnion 'all' is supported

  // maximum number of times to repeat the iterative relational expression,
  // -1 means no limit, 0 means only seed will be evaluated
  protected final int maxRep;

  //~ Constructors -----------------------------------------------------------
  protected RepeatUnion(
          int maxRep,
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode seed,
          RelNode iterative) {

    super(cluster, traitSet, seed, iterative);
    if (maxRep < -1) {
      throw new IllegalArgumentException("Wrong maxRep value");
    }
    this.maxRep = maxRep;
  }


  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // TODO implement a more accurate row count?
    double seedRowCount = getSeedRel().estimateRowCount(mq);
    if (maxRep == 0) {
      return seedRowCount;
    }
    return seedRowCount
              + getIterativeRel().estimateRowCount(mq) * (maxRep != -1 ? maxRep : 10);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (maxRep != -1) {
      pw.item("maxRep", maxRep);
    }
    return pw.item("all", all);
  }

  public int getMaxRep() {
    return maxRep;
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
