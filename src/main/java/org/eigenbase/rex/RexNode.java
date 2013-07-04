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

import org.eigenbase.reltype.*;


/**
 * Row expression.
 *
 * <p>Every row-expression has a type. (Compare with {@link
 * org.eigenbase.sql.SqlNode}, which is created before validation, and therefore
 * types may not be available.)</p>
 *
 * <p>Some common row-expressions are: {@link RexLiteral} (constant value),
 * {@link RexVariable} (variable), {@link RexCall} (call to operator with
 * operands). Expressions are generally created using a {@link RexBuilder}
 * factory.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 22, 2003
 */
public abstract class RexNode
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RexNode [] EMPTY_ARRAY = new RexNode[0];

    //~ Instance fields --------------------------------------------------------

    protected String digest;

    //~ Methods ----------------------------------------------------------------

    public abstract RelDataType getType();

    public abstract RexNode clone();

    /**
     * Returns whether this expression always returns true. (Such as if this
     * expression is equal to the literal <code>TRUE</code>.)
     */
    public boolean isAlwaysTrue()
    {
        return false;
    }

    public boolean isA(RexKind kind)
    {
        return (getKind() == kind) || kind.includes(getKind());
    }

    /**
     * Returns the kind of node this is.
     *
     * @return A {@link RexKind} value, never null
     *
     * @post return != null
     */
    public RexKind getKind()
    {
        return RexKind.Other;
    }

    public String toString()
    {
        return digest;
    }

    /**
     * Accepts a visitor, dispatching to the right overloaded {@link
     * RexVisitor#visitInputRef visitXxx} method.
     *
     * <p>Also see {@link RexProgram#apply(RexVisitor, java.util.List, RexNode)},
     * which applies a visitor to several expressions simultaneously.
     */
    public abstract <R> R accept(RexVisitor<R> visitor);
}

// End RexNode.java
