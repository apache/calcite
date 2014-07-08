/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.rel.rules;

import java.util.*;

import org.apache.optiq.rel.*;
import org.apache.optiq.rel.metadata.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.*;
import org.apache.optiq.rex.*;

/**
 * PushFilterPastTableFunctionRule implements the rule for pushing a
 * {@link FilterRel} past a {@link TableFunctionRel}.
 */
public class PushFilterPastTableFunctionRule extends RelOptRule {
  public static final PushFilterPastTableFunctionRule INSTANCE =
      new PushFilterPastTableFunctionRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastTableFunctionRule.
   */
  private PushFilterPastTableFunctionRule() {
    super(
        operand(
            FilterRel.class,
            operand(TableFunctionRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel filterRel = call.rel(0);
    TableFunctionRel funcRel = call.rel(1);
    Set<RelColumnMapping> columnMappings = funcRel.getColumnMappings();
    if ((columnMappings == null) || (columnMappings.isEmpty())) {
      // No column mapping information, so no pushdown
      // possible.
      return;
    }

    List<RelNode> funcInputs = funcRel.getInputs();
    if (funcInputs.size() != 1) {
      // TODO:  support more than one relational input; requires
      // offsetting field indices, similar to join
      return;
    }
    // TODO:  support mappings other than 1-to-1
    if (funcRel.getRowType().getFieldCount()
        != funcInputs.get(0).getRowType().getFieldCount()) {
      return;
    }
    for (RelColumnMapping mapping : columnMappings) {
      if (mapping.iInputColumn != mapping.iOutputColumn) {
        return;
      }
      if (mapping.derived) {
        return;
      }
    }
    final List<RelNode> newFuncInputs = new ArrayList<RelNode>();
    final RelOptCluster cluster = funcRel.getCluster();
    final RexNode condition = filterRel.getCondition();

    // create filters on top of each func input, modifying the filter
    // condition to reference the child instead
    RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    List<RelDataTypeField> origFields = funcRel.getRowType().getFieldList();
    // TODO:  these need to be non-zero once we
    // support arbitrary mappings
    int[] adjustments = new int[origFields.size()];
    for (RelNode funcInput : funcInputs) {
      RexNode newCondition =
          condition.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  origFields,
                  funcInput.getRowType().getFieldList(),
                  adjustments));
      newFuncInputs.add(
          new FilterRel(cluster, funcInput, newCondition));
    }

    // create a new UDX whose children are the filters created above
    TableFunctionRel newFuncRel =
        new TableFunctionRel(
            cluster,
            newFuncInputs,
            funcRel.getCall(),
            funcRel.getElementType(), funcRel.getRowType(),
            columnMappings);
    call.transformTo(newFuncRel);
  }
}

// End PushFilterPastTableFunctionRule.java
