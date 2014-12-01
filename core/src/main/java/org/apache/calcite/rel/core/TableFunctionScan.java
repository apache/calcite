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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Relational expression that calls a table-valued function.
 *
 * <p>The function returns a result set.
 * It can appear as a leaf in a query tree,
 * or can be applied to relational inputs.
 *
 * @see org.apache.calcite.rel.logical.LogicalTableFunctionScan
 */
public abstract class TableFunctionScan extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  private final RexNode rexCall;

  private final Type elementType;

  private ImmutableList<RelNode> inputs;

  protected final ImmutableSet<RelColumnMapping> columnMappings;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>TableFunctionScan</code>.
   *
   * @param cluster        Cluster that this relational expression belongs to
   * @param inputs         0 or more relational inputs
   * @param rexCall        function invocation expression
   * @param elementType    element type of the collection that will implement
   *                       this table
   * @param rowType        row type produced by function
   * @param columnMappings column mappings associated with this function
   */
  protected TableFunctionScan(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    super(cluster, traits);
    this.rexCall = rexCall;
    this.elementType = elementType;
    this.rowType = rowType;
    this.inputs = ImmutableList.copyOf(inputs);
    this.columnMappings =
        columnMappings == null ? null : ImmutableSet.copyOf(columnMappings);
  }

  /**
   * Creates a TableFunctionScan by parsing serialized output.
   */
  protected TableFunctionScan(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInputs(),
        input.getExpression("invocation"), (Type) input.get("elementType"),
        input.getRowType("rowType"),
        ImmutableSet.<RelColumnMapping>of());
  }

  //~ Methods ----------------------------------------------------------------

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(rexCall);
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    final List<RelNode> newInputs = new ArrayList<RelNode>(inputs);
    newInputs.set(ordinalInParent, p);
    inputs = ImmutableList.copyOf(newInputs);
    recomputeDigest();
  }

  @Override public double getRows() {
    // Calculate result as the sum of the input rowcount estimates,
    // assuming there are any, otherwise use the superclass default.  So
    // for a no-input UDX, behave like an AbstractRelNode; for a one-input
    // UDX, behave like a SingleRel; for a multi-input UDX, behave like
    // UNION ALL.  TODO jvs 10-Sep-2007: UDX-supplied costing metadata.
    if (inputs.size() == 0) {
      return super.getRows();
    }
    double nRows = 0.0;
    for (RelNode input : inputs) {
      Double d = RelMetadataQuery.getRowCount(input);
      if (d != null) {
        nRows += d;
      }
    }
    return nRows;
  }

  public RexNode getCall() {
    // NOTE jvs 7-May-2006:  Within this rexCall, instances
    // of RexInputRef refer to entire input RelNodes rather
    // than their fields.
    return rexCall;
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    pw.item("invocation", rexCall)
      .item("rowType", rowType);
    if (elementType != null) {
      pw.item("elementType", elementType);
    }
    return pw;
  }

  /**
   * @return set of mappings known for this table function, or null if unknown
   * (not the same as empty!)
   */
  public Set<RelColumnMapping> getColumnMappings() {
    return columnMappings;
  }

  /**
   * Returns element type of the collection that will implement this table.
   * @return element type of the collection that will implement this table
   */
  public Type getElementType() {
    return elementType;
  }
}

// End TableFunctionScan.java
