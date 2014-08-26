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
package org.eigenbase.rel;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.metadata.RelColumnMapping;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * <code>TableFunctionRel</code> represents a call to a function which returns a
 * result set. Currently, it can only appear as a leaf in a query tree, but
 * eventually we will extend it to take relational inputs.
 */
public class TableFunctionRel extends TableFunctionRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>TableFunctionRel</code>.
   *
   * @param cluster        Cluster that this relational expression belongs to
   * @param inputs         0 or more relational inputs
   * @param rexCall        function invocation expression
   * @param elementType    element type of the collection that will implement
   *                       this table
   * @param rowType        row type produced by function
   * @param columnMappings column mappings associated with this function
   */
  public TableFunctionRel(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        inputs,
        rexCall,
        elementType,
        rowType,
        columnMappings);
  }

  /**
   * Creates a TableFunctionRel by parsing serialized output.
   */
  public TableFunctionRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public TableFunctionRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new TableFunctionRel(
        getCluster(),
        inputs,
        getCall(),
        getElementType(), getRowType(),
        columnMappings);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 8-Jan-2006:  what is supposed to be here
    // for an abstract rel?
    return planner.getCostFactory().makeHugeCost();
  }
}

// End TableFunctionRel.java
