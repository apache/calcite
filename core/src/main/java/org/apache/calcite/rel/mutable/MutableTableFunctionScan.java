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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Mutable equivalent of
 * {@link org.apache.calcite.rel.core.TableFunctionScan}. */
public class MutableTableFunctionScan extends MutableMultiRel {
  public final RexNode rexCall;
  public final Type elementType;
  public final Set<RelColumnMapping> columnMappings;

  private MutableTableFunctionScan(RelOptCluster cluster,
      RelDataType rowType, List<MutableRel> inputs, RexNode rexCall,
      Type elementType, Set<RelColumnMapping> columnMappings) {
    super(cluster, rowType, MutableRelType.TABLE_FUNCTION_SCAN, inputs);
    this.rexCall = rexCall;
    this.elementType = elementType;
    this.columnMappings = columnMappings;
  }

  /**
   * Creates a MutableTableFunctionScan.
   *
   * @param cluster         Cluster that this relational expression belongs to
   * @param rowType         Row type
   * @param inputs          Input relational expressions
   * @param rexCall         Function invocation expression
   * @param elementType     Element type of the collection that will implement
   *                        this table
   * @param columnMappings  Column mappings associated with this function
   */
  public static MutableTableFunctionScan of(RelOptCluster cluster,
      RelDataType rowType, List<MutableRel> inputs, RexNode rexCall,
      Type elementType, Set<RelColumnMapping> columnMappings) {
    return new MutableTableFunctionScan(
        cluster, rowType, inputs, rexCall, elementType, columnMappings);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableTableFunctionScan
        && STRING_EQUIVALENCE.equivalent(rexCall,
            ((MutableTableFunctionScan) obj).rexCall)
        && Objects.equals(elementType,
            ((MutableTableFunctionScan) obj).elementType)
        && Objects.equals(columnMappings,
            ((MutableTableFunctionScan) obj).columnMappings)
        && inputs.equals(((MutableTableFunctionScan) obj).getInputs());
  }

  @Override public int hashCode() {
    return Objects.hash(inputs,
        STRING_EQUIVALENCE.hash(rexCall), elementType, columnMappings);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    buf.append("TableFunctionScan(rexCall: ").append(rexCall);
    if (elementType != null) {
      buf.append(", elementType: ").append(elementType);
    }
    return buf.append(")");
  }

  @Override public MutableRel clone() {
    // TODO Auto-generated method stub
    return MutableTableFunctionScan.of(cluster, rowType,
        cloneChildren(), rexCall, elementType, columnMappings);
  }

}

// End MutableTableFunctionScan.java
