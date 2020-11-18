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

package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;


public class ArrowFilterTableScanRule extends RelRule<ArrowFilterTableScanRule.Config> {

  protected ArrowFilterTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final ArrowTableScan scan = call.rel(1);
    int[] fields = {0, 1, 2};

    final RexNode condition = filter.getCondition();
    final SqlKind operator = condition.getKind();
    final RexCall rexCall = (RexCall) condition;
    RexNode left = rexCall.getOperands().get(0);
    RexNode right = rexCall.getOperands().get(1);

    call.transformTo(
        new ArrowTableScan(
            scan.getCluster(),
            scan.getTable(),
            scan.arrowTable,
            fields,
            operator,
            ((RexInputRef) left).getIndex(),
            ((RexLiteral) right).getValue2()));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    ArrowFilterTableScanRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class).oneInput(b1 ->
                b1.operand(ArrowTableScan.class).noInputs()))
        .as(ArrowFilterTableScanRule.Config.class);

    @Override default ArrowFilterTableScanRule toRule() {
      return new ArrowFilterTableScanRule(this);
    }
  }
}
