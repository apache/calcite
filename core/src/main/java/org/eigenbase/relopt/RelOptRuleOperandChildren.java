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
package org.eigenbase.relopt;

import com.google.common.collect.ImmutableList;

/**
 * Children of a {@link org.eigenbase.relopt.RelOptRuleOperand} and the policy
 * for matching them.
*/
public class RelOptRuleOperandChildren {
    final RelOptRuleOperandChildPolicy policy;
    final ImmutableList<RelOptRuleOperand> operands;

    public RelOptRuleOperandChildren(
        RelOptRuleOperandChildPolicy policy,
        ImmutableList<RelOptRuleOperand> operands)
    {
        this.policy = policy;
        this.operands = operands;
    }

    public static RelOptRuleOperandChildren of(
        RelOptRuleOperandChildPolicy policy,
        RelOptRuleOperand[] operands)
    {
        return new RelOptRuleOperandChildren(
            policy,
            ImmutableList.copyOf(operands));
    }

    public static RelOptRuleOperandChildren of(
        RelOptRuleOperandChildPolicy policy,
        RelOptRuleOperand first,
        RelOptRuleOperand... others)
    {
        return new RelOptRuleOperandChildren(
            policy,
            ImmutableList.<RelOptRuleOperand>builder().add(first)
                .add(others).build());
    }
}

// End RelOptRuleOperandChildren.java
