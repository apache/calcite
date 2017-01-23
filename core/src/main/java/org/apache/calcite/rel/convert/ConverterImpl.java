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
package org.apache.calcite.rel.convert;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Abstract implementation of {@link Converter}.
 */
public abstract class ConverterImpl extends SingleRel
    implements Converter {
  //~ Instance fields --------------------------------------------------------

  protected RelTraitSet inTraits;
  protected final RelTraitDef traitDef;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ConverterImpl.
   *
   * @param cluster  planner's cluster
   * @param traitDef the RelTraitDef this converter converts
   * @param traits   the output traits of this converter
   * @param child    child rel (provides input traits)
   */
  protected ConverterImpl(
      RelOptCluster cluster,
      RelTraitDef traitDef,
      RelTraitSet traits,
      RelNode child) {
    super(cluster, traits, child);
    this.inTraits = child.getTraitSet();
    this.traitDef = traitDef;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double dRows = mq.getRowCount(getInput());
    double dCpu = dRows;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Deprecated // to be removed before 2.0
  protected Error cannotImplement() {
    return new AssertionError(getClass() + " cannot convert from "
        + inTraits + " traits");
  }

  public RelTraitSet getInputTraits() {
    return inTraits;
  }

  public RelTraitDef getTraitDef() {
    return traitDef;
  }

}

// End ConverterImpl.java
