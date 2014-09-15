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
package org.eigenbase.rel.rules;

import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * Rule that, given a {@link ProjectRelBase} node that merely returns its input,
 * converts the node into its child.
 *
 * <p>For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.</p>
 *
 * @see org.eigenbase.rel.rules.RemoveTrivialCalcRule
 * @see org.eigenbase.rel.rules.MergeProjectRule
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
      new RelOptRuleOperand(ProjectRelBase.class, null, any()) {
        @Override public boolean matches(RelNode rel) {
          return super.matches(rel)
              && isTrivial((ProjectRelBase) rel);
        }
      });
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    ProjectRelBase project = call.rel(0);
    assert isTrivial(project);
    RelNode stripped = project.getChild();
    RelNode child = call.getPlanner().register(stripped, project);
    call.transformTo(child);
  }

  /**
   * Returns the child of a project if the project is trivial, otherwise
   * the project itself.
   */
  public static RelNode strip(ProjectRelBase project) {
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
      List<? extends RexNode> exps,
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
