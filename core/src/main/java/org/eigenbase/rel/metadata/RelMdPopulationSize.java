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
import org.eigenbase.rex.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.util.BitSets;

/**
 * RelMdPopulationSize supplies a default implementation of {@link
 * RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 */
public class RelMdPopulationSize {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.POPULATION_SIZE.method, new RelMdPopulationSize());

  //~ Constructors -----------------------------------------------------------

  private RelMdPopulationSize() {}

  //~ Methods ----------------------------------------------------------------

  public Double getPopulationSize(FilterRelBase rel, BitSet groupKey) {
    return RelMetadataQuery.getPopulationSize(
        rel.getChild(),
        groupKey);
  }

  public Double getPopulationSize(SortRel rel, BitSet groupKey) {
    return RelMetadataQuery.getPopulationSize(
        rel.getChild(),
        groupKey);
  }

  public Double getPopulationSize(UnionRelBase rel, BitSet groupKey) {
    Double population = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double subPop = RelMetadataQuery.getPopulationSize(input, groupKey);
      if (subPop == null) {
        return null;
      }
      population += subPop;
    }
    return population;
  }

  public Double getPopulationSize(JoinRelBase rel, BitSet groupKey) {
    return RelMdUtil.getJoinPopulationSize(rel, groupKey);
  }

  public Double getPopulationSize(SemiJoinRel rel, BitSet groupKey) {
    return RelMetadataQuery.getPopulationSize(
        rel.getLeft(),
        groupKey);
  }

  public Double getPopulationSize(AggregateRelBase rel, BitSet groupKey) {
    BitSet childKey = new BitSet();
    RelMdUtil.setAggChildKeys(groupKey, rel, childKey);
    return RelMetadataQuery.getPopulationSize(
        rel.getChild(),
        childKey);
  }

  public Double getPopulationSize(ValuesRelBase rel, BitSet groupKey) {
    // assume half the rows are duplicates
    return rel.getRows() / 2;
  }

  public Double getPopulationSize(ProjectRelBase rel, BitSet groupKey) {
    BitSet baseCols = new BitSet();
    BitSet projCols = new BitSet();
    List<RexNode> projExprs = rel.getProjects();
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

    Double population =
        RelMetadataQuery.getPopulationSize(
            rel.getChild(),
            baseCols);
    if (population == null) {
      return null;
    }

    // No further computation required if the projection expressions are
    // all column references
    if (projCols.cardinality() == 0) {
      return population;
    }

    for (int bit : BitSets.toIter(projCols)) {
      Double subRowCount =
          RelMdUtil.cardOfProjExpr(rel, projExprs.get(bit));
      if (subRowCount == null) {
        return null;
      }
      population *= subRowCount;
    }

    // REVIEW zfong 6/22/06 - Broadbase did not have the call to
    // numDistinctVals.  This is needed; otherwise, population can be
    // larger than the number of rows in the RelNode.
    return RelMdUtil.numDistinctVals(
        population,
        RelMetadataQuery.getRowCount(rel));
  }

  // Catch-all rule when none of the others apply.
  public Double getPopulationSize(RelNode rel, BitSet groupKey) {
    // if the keys are unique, return the row count; otherwise, we have
    // no further information on which to return any legitimate value

    // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
    // unique key, which would result in the population being larger
    // than the total rows in the relnode
    boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(rel, groupKey);
    if (uniq) {
      return RelMetadataQuery.getRowCount(rel);
    }

    return null;
  }
}

// End RelMdPopulationSize.java
