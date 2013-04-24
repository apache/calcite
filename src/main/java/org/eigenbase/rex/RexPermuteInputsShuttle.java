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
package org.eigenbase.rex;

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.util.mapping.Mappings;


/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @author jhyde
 * @version $Id$
 * @see RexPermutationShuttle
 */
public class RexPermuteInputsShuttle
    extends RexShuttle
{
    //~ Instance fields --------------------------------------------------------

    private final Mappings.TargetMapping mapping;
    private final List<RelDataTypeField> fields =
        new ArrayList<RelDataTypeField>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexPermuteInputsShuttle.
     *
     * <p>The mapping provides at most one target for every source. If a source
     * has no targets and is referenced in the expression,
     * {@link org.eigenbase.util.mapping.Mappings.TargetMapping#getTarget(int)}
     * will give an error. Otherwise the mapping gives a unique target.
     *
     * @param mapping Mapping
     * @param inputs Input relational expressions
     */
    public RexPermuteInputsShuttle(
        Mappings.TargetMapping mapping,
        RelNode... inputs)
    {
        this.mapping = mapping;
        for (RelNode input : inputs) {
            fields.addAll(input.getRowType().getFieldList());
        }
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public RexNode visitInputRef(RexInputRef local)
    {
        final int index = local.getIndex();
        int target = mapping.getTarget(index);
        return new RexInputRef(
            target,
            local.getType());
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (call.getOperator() == RexBuilder.GET_OPERATOR) {
            final String name =
                (String) ((RexLiteral) call.getOperands()[1]).getValue2();
            final int i = lookup(fields, name);
            if (i >= 0) {
                return new RexInputRef(i, fields.get(i).getType());
            }
        }
        return super.visitCall(call);
    }

    private static int lookup(List<RelDataTypeField> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            final RelDataTypeField field = fields.get(i);
            if (field.getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}

// End RexPermuteInputsShuttle.java
