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
package org.eigenbase.rel.convert;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

/**
 * Abstract implementation of {@link org.eigenbase.rel.convert.ConverterRel}.
 */
public abstract class ConverterRelImpl extends SingleRel
    implements ConverterRel {
  //~ Instance fields --------------------------------------------------------

  protected RelTraitSet inTraits;
  protected final RelTraitDef traitDef;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ConverterRelImpl.
   *
   * @param cluster  planner's cluster
   * @param traitDef the RelTraitDef this converter converts
   * @param traits   the output traits of this converter
   * @param child    child rel (provides input traits)
   */
  protected ConverterRelImpl(
      RelOptCluster cluster,
      RelTraitDef traitDef,
      RelTraitSet traits,
      RelNode child) {
    super(cluster, traits, child);
    this.inTraits = child.getTraitSet();
    this.traitDef = traitDef;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelNode
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(getChild());
    double dCpu = dRows;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  protected Error cannotImplement() {
    return Util.newInternal(
        getClass() + " cannot convert from "
            + inTraits + " traits");
  }

  public boolean isDistinct() {
    return getChild().isDistinct();
  }

  public RelTraitSet getInputTraits() {
    return inTraits;
  }

  public RelTraitDef getTraitDef() {
    return traitDef;
  }

}

// End ConverterRelImpl.java
