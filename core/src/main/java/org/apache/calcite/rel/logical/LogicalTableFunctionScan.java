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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.TableFunctionScan}
 * not targeted at any particular engine or calling convention.
 */
public class LogicalTableFunctionScan extends TableFunctionScan {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>LogicalTableFunctionScan</code>.
   *
   * @param cluster        Cluster that this relational expression belongs to
   * @param inputs         0 or more relational inputs
   * @param traitSet       Trait set
   * @param rexCall        Function invocation expression
   * @param elementType    Element type of the collection that will implement
   *                       this table
   * @param rowType        Row type produced by function
   * @param columnMappings Column mappings associated with this function
   */
  public LogicalTableFunctionScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType,
        columnMappings);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTableFunctionScan(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    this(cluster, cluster.traitSetOf(Convention.NONE), inputs, rexCall,
        elementType, rowType, columnMappings);
  }

  /**
   * Creates a LogicalTableFunctionScan by parsing serialized output.
   */
  public LogicalTableFunctionScan(RelInput input) {
    super(input);
  }

  /** Creates a LogicalTableFunctionScan. */
  public static LogicalTableFunctionScan create(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableFunctionScan(cluster, traitSet, inputs, rexCall,
        elementType, rowType, columnMappings);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType,
      RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalTableFunctionScan(
        getCluster(),
        traitSet,
        inputs,
        rexCall,
        elementType,
        rowType,
        columnMappings);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // REVIEW jvs 8-Jan-2006:  what is supposed to be here
    // for an abstract rel?
    return planner.getCostFactory().makeHugeCost();
  }
}

// End LogicalTableFunctionScan.java
