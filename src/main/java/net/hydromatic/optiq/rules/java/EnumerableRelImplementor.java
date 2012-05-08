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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;

import org.eigenbase.rel.RelImplementorImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelImplementor;
import org.eigenbase.rex.RexBuilder;

/**
 * Subclass of {@link RelImplementor} for relational operators
 * of {@link org.eigenbase.relopt.CallingConvention#ENUMERABLE} calling
 * convention.
 *
 * @author jhyde
 */
public class EnumerableRelImplementor extends RelImplementorImpl {
    public EnumerableRelImplementor(RexBuilder rexBuilder) {
        super(rexBuilder);
    }

    public Expression visitChild(
        EnumerableRel parent,
        int ordinal,
        EnumerableRel child)
    {
        return (Expression) super.visitChild(parent, ordinal, child);
    }

    public Expression visitChildInternal(RelNode child, int ordinal)
    {
        return ((EnumerableRel) child).implement(this);
    }
}

// End EnumerableRelImplementor.java
