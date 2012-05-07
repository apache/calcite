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
package org.eigenbase.oj.util;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A row expression which is implemented by an underlying Java expression.
 *
 * <p>This is a leaf node of a {@link RexNode} tree, but the Java expression,
 * represented by a {@link Expression} object, may be complex.</p>
 *
 * @author jhyde
 * @version $Id$
 * @see JavaRexBuilder
 * @since Nov 23, 2003
 */
public class JavaRowExpression
    extends RexNode
{
    //~ Instance fields --------------------------------------------------------

    final Environment env;
    private final RelDataType type;
    private final Expression expression;

    //~ Constructors -----------------------------------------------------------

    public JavaRowExpression(
        Environment env,
        RelDataType type,
        Expression expression)
    {
        this.env = env;
        this.type = type;
        this.expression = expression;
        this.digest = "Java(" + expression + ")";
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isAlwaysTrue()
    {
        return expression == Literal.constantTrue();
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        if (visitor instanceof RexShuttle) {
            return (R) this;
        }
        return null;
    }

    public RelDataType getType()
    {
        return type;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public JavaRowExpression clone()
    {
        return new JavaRowExpression(env, type, expression);
    }
}

// End JavaRowExpression.java
