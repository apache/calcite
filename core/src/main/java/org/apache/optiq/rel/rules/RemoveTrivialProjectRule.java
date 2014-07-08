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

import java.util.List;

import org.apache.optiq.rel.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.*;
import org.apache.optiq.rex.*;

/**
 * Rule which, given a {@link ProjectRel} node which merely returns its input,
 * converts the node into its child.
 *
 * <p>For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.</p>
 *
 * @see org.apache.optiq.rel.rules.RemoveTrivialCalcRule
 */
public class RemoveTrivialProjectRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final RemoveTrivialProjectRule INSTANCE =
      new RemoveTrivialProjectRule();

  //~ Constructors -----------------------------------------------------------

  private RemoveTrivialProjectRule() {
    // Create a specialized operand to detect non-matches early. This keeps
    // the rule queue short.
    super(
      new RelOptRuleOperand(ProjectRel.class, null, any()) {
        @Override public boolean matches(RelNode rel) {
          return super.matches(rel)
              && isTrivial((ProjectRel) rel);
        }
      });
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    ProjectRel project = call.rel(0);
    assert isTrivial(project);
    RelNode stripped = project.getChild();
    RelNode child = call.getPlanner().register(stripped, project);
    call.transformTo(
        convert(
            child,
            project.getTraitSet()));
  }

  /**
   * Returns the child of a project if the project is trivial, otherwise
   * the project itself.
   */
  public static RelNode strip(ProjectRel project) {
    return isTrivial(project) ? project.getChild() : project;
  }

  public static boolean isTrivial(ProjectRelBase project) {
    RelNode child = project.getChild();
    final RelDataType childRowType = child.getRowType();
    if (!childRowType.isStruct()) {
      return false;
    }
    if (!project.isBoxed()) {
      return false;
    }
    if (!isIdentity(
        project.getProjects(),
        project.getRowType(),
        childRowType)) {
      return false;
    }
    return true;
  }

  public static boolean isIdentity(
      List<RexNode> exps,
      RelDataType rowType,
      RelDataType childRowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RelDataTypeField> childFields = childRowType.getFieldList();
    int fieldCount = childFields.size();
    if (exps.size() != fieldCount) {
      return false;
    }
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
      RexInputRef var = (RexInputRef) exp;
      if (var.getIndex() != i) {
        return false;
      }
      if (!fields.get(i).getName().equals(childFields.get(i).getName())) {
        return false;
      }
    }
    return true;
  }
}

// End RemoveTrivialProjectRule.java
