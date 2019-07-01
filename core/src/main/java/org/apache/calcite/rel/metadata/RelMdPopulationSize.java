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
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * RelMdPopulationSize supplies a default implementation of
 * {@link RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 */
public class RelMdPopulationSize
    implements MetadataHandler<BuiltInMetadata.PopulationSize> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.POPULATION_SIZE.method, new RelMdPopulationSize());

  //~ Constructors -----------------------------------------------------------

  private RelMdPopulationSize() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.PopulationSize> getDef() {
    return BuiltInMetadata.PopulationSize.DEF;
  }

  public Double getPopulationSize(Filter rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Sort rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Exchange rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Union rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    double population = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double subPop = mq.getPopulationSize(input, groupKey);
      if (subPop == null) {
        return null;
      }
      population += subPop;
    }
    return population;
  }

  public Double getPopulationSize(Join rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    return RelMdUtil.getJoinPopulationSize(mq, rel, groupKey);
  }

  public Double getPopulationSize(Aggregate rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
    RelMdUtil.setAggChildKeys(groupKey, rel, childKey);
    return mq.getPopulationSize(rel.getInput(), childKey.build());
  }

  public Double getPopulationSize(Values rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    // assume half the rows are duplicates
    return rel.estimateRowCount(mq) / 2;
  }

  public Double getPopulationSize(Project rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    ImmutableBitSet.Builder baseCols = ImmutableBitSet.builder();
    ImmutableBitSet.Builder projCols = ImmutableBitSet.builder();
    List<RexNode> projExprs = rel.getProjects();
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

    Double population =
        mq.getPopulationSize(rel.getInput(), baseCols.build());
    if (population == null) {
      return null;
    }

    // No further computation required if the projection expressions are
    // all column references
    if (projCols.cardinality() == 0) {
      return population;
    }

    for (int bit : projCols.build()) {
      Double subRowCount =
          RelMdUtil.cardOfProjExpr(mq, rel, projExprs.get(bit));
      if (subRowCount == null) {
        return null;
      }
      population *= subRowCount;
    }

    // REVIEW zfong 6/22/06 - Broadbase did not have the call to
    // numDistinctVals.  This is needed; otherwise, population can be
    // larger than the number of rows in the RelNode.
    return RelMdUtil.numDistinctVals(population, mq.getRowCount(rel));
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getPopulationSize(RelNode, ImmutableBitSet)
   */
  public Double getPopulationSize(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey) {
    // if the keys are unique, return the row count; otherwise, we have
    // no further information on which to return any legitimate value

    // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
    // unique key, which would result in the population being larger
    // than the total rows in the relnode
    boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey);
    if (uniq) {
      return mq.getRowCount(rel);
    }

    return null;
  }
}

// End RelMdPopulationSize.java
