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

import java.util.*;

/**
 * Enumeration of some important types of row-expression.
 *
 * <p>The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a common
 * operator such as '=', use {@link RexNode#isA}:
 *
 * <blockquote>
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Equals RexKind.Equals})</pre>
 * </blockquote>
 *
 * To identify a category of expressions, you can use {@link RexNode#isA} with
 * an aggregate RexKind. The following expression will return <code>true</code>
 * for calls to '=' and '&gt;=', but <code>false</code> for the constant '5', or
 * a call to '+':
 *
 * <blockquote>
 * <pre>exp.{@link RexNode#isA isA}({@link RexKind#Comparison RexKind.Comparison})</pre>
 * </blockquote>
 *
 * To quickly choose between a number of options, use a switch statement:
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case {@link #Equals}:
 *     ...;
 * case {@link #NotEquals}:
 *     ...;
 * default:
 *     throw {@link org.eigenbase.util.Util#unexpected Util.unexpected}(exp.getKind());
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public enum RexKind
{
    /**
     * No operator in particular. This is the default kind.
     */
    Other,

    /**
     * The equals operator, "=".
     */
    Equals,

    /**
     * The not-equals operator, "&#33;=" or "&lt;&gt;".
     */
    NotEquals,

    /**
     * The greater-than operator, "&gt;".
     */
    GreaterThan,

    /**
     * The greater-than-or-equal operator, "&gt;=".
     */
    GreaterThanOrEqual,

    /**
     * The less-than operator, "&lt;".
     */
    LessThan,

    /**
     * The less-than-or-equal operator, "&lt;=".
     */
    LessThanOrEqual,

    /**
     * A comparison operator ({@link #Equals}, {@link #GreaterThan}, etc.).
     * Comparisons are always a {@link RexCall} with 2 arguments.
     */
    Comparison(
        new RexKind[] {
            Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan,
            LessThanOrEqual
        }),

    /**
     * The logical "AND" operator.
     */
    And,

    /**
     * The logical "OR" operator.
     */
    Or,

    /**
     * The logical "NOT" operator.
     */
    Not,

    /**
     * A logical operator ({@link #And}, {@link #Or}, {@link #Not}).
     */
    Logical(new RexKind[] { And, Or, Not }),

    /**
     * The arithmetic division operator, "/".
     */
    Divide,

    /**
     * The arithmetic minus operator, "-".
     *
     * @see #MinusPrefix
     */
    Minus,

    /**
     * The arithmetic plus operator, "+".
     */
    Plus,

    /**
     * The unary minus operator, as in "-1".
     *
     * @see #Minus
     */
    MinusPrefix,

    /**
     * The arithmetic multiplication operator, "*".
     */
    Times,

    /**
     * An arithmetic operator ({@link #Divide}, {@link #Minus}, {@link
     * #MinusPrefix}, {@link #Plus}, {@link #Times}).
     */
    Arithmetic(new RexKind[] { Divide, Minus, MinusPrefix, Plus, Times }),

    /**
     * The field access operator, ".".
     */
    FieldAccess,

    /**
     * The string concatenation operator, "||".
     */
    Concat,

    /**
     * The substring function.
     */

    // REVIEW (jhyde, 2004/1/26) We should obsolete Substr. RexKind values are
    // so that the validator and optimizer can quickly recognize special
    // syntactic cetegories, and there's nothing particularly special about
    // Substr. For the mapping of sql->rex, and rex->calc, just use its name or
    // signature.
    Substr,

    /**
     * The row constructor operator.
     */
    Row,

    /**
     * The IS NULL operator.
     */
    IsNull,

    /**
     * An identifier.
     */
    Identifier,

    /**
     * A literal.
     */
    Literal,

    /**
     * The VALUES operator.
     */
    Values,

    /**
     * The IS TRUE operator.
     */
    IsTrue,

    /**
     * The IS FALSE operator.
     */
    IsFalse,

    /**
     * A dynamic parameter.
     */
    DynamicParam, Cast, Trim,

    /**
     * The LIKE operator.
     */
    Like,

    /**
     * The SIMILAR operator.
     */
    Similar,

    /**
     * The MULTISET Query Constructor
     */
    MultisetQueryConstructor,

    /**
     * The MAP value constructor
     */
    MapValueConstructor,

    /**
     * The ARRAY value constructor
     */
    ArrayValueConstructor,

    /**
     * NEW invocation
     */
    NewSpecification,

    /**
     * The internal REINTERPRET operator
     */
    Reinterpret,

    Descending,
    NullsFirst,
    NullsLast;

    private final Set<RexKind> otherKinds;

    /**
     * Creates a kind.
     */
    RexKind()
    {
        otherKinds = Collections.emptySet();
    }

    /**
     * Creates a kind which includes other kinds.
     */
    RexKind(RexKind [] others)
    {
        otherKinds = new HashSet<RexKind>();
        for (RexKind other : others) {
            otherKinds.add(other);
            otherKinds.addAll(other.otherKinds);
        }
    }

    public boolean includes(RexKind kind)
    {
        return (kind == this) || otherKinds.contains(kind);
    }
}

// End RexKind.java
