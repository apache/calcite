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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Rule which, given a {@link ProjectRel} node which merely returns its input,
 * converts the node into its child.
 *
 * <p>For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.</p>
 *
 * @see org.eigenbase.rel.rules.RemoveTrivialCalcRule
 */
public class RemoveTrivialProjectRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RemoveTrivialProjectRule instance =
        new RemoveTrivialProjectRule();

    //~ Constructors -----------------------------------------------------------

    private RemoveTrivialProjectRule()
    {
        super(any(ProjectRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = call.rel(0);
        RelNode child = project.getChild();
        final RelDataType childRowType = child.getRowType();
        if (!childRowType.isStruct()) {
            return;
        }
        if (!project.isBoxed()) {
            return;
        }
        if (!isIdentity(
                project.getProjectExps(),
                project.getRowType(),
                childRowType))
        {
            return;
        }
        child = call.getPlanner().register(child, project);
        call.transformTo(
            convert(
                child,
                project.getTraitSet()));
    }

    public static boolean isIdentity(
        RexNode [] exps,
        RelDataType rowType,
        RelDataType childRowType)
    {
        RelDataTypeField [] fields = rowType.getFields();
        RelDataTypeField [] childFields = childRowType.getFields();
        int fieldCount = childFields.length;
        if (exps.length != fieldCount) {
            return false;
        }
        for (int i = 0; i < exps.length; i++) {
            RexNode exp = exps[i];
            if (exp instanceof RexInputRef) {
                RexInputRef var = (RexInputRef) exp;
                if (var.getIndex() != i) {
                    return false;
                }
                if (!fields[i].getName().equals(childFields[i].getName())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}

// End RemoveTrivialProjectRule.java
