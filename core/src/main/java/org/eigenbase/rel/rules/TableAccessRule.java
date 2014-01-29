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
package org.eigenbase.rel.rules;

import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;

/**
 * TableAccessRule converts a TableAccessRel to the result of calling {@link
 * RelOptTable#toRel}.
 */
public class TableAccessRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final TableAccessRule INSTANCE = new TableAccessRule();

  //~ Constructors -----------------------------------------------------------

  private TableAccessRule() {
    super(operand(TableAccessRel.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final TableAccessRel oldRel = call.rel(0);
    RelNode newRel =
        oldRel.getTable().toRel(
            new RelOptTable.ToRelContext() {
              public RelOptCluster getCluster() {
                return oldRel.getCluster();
              }

              public RelNode expandView(
                  RelDataType rowType,
                  String queryString,
                  List<String> schemaPath) {
                throw new UnsupportedOperationException();
              }
            });
    call.transformTo(newRel);
  }
}

// End TableAccessRule.java
