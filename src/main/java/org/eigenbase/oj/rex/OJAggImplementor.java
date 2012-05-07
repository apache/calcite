/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.oj.rex;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;


/**
 * Translates a call to an {@link org.eigenbase.rel.Aggregation} into OpenJava
 * code.
 *
 * <p>Implementors are held in a {@link OJRexImplementorTable}.
 *
 * @author jhyde
 * @version $Id$
 */
public interface OJAggImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Generates the expression which gets called when a new total is created.
     * For <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(0)</code>.
     */
    Expression implementStart(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateCall call);

    /**
     * Generates code to create a new total and to add the first value. For
     * <code>sum(x)</code>, this looks like <code>new
     * saffron.runtime.Holder.int_Holder(x)</code>.
     */
    Expression implementStartAndNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateCall call);

    /**
     * Returns whether this aggregation can merge together two accumulators.
     *
     * <p>For example, <code>COUNT</code> can (you just add the accumulators);
     * <code>AVG</code> and <code>MEDIAN</code> cannot.
     *
     * @return whether this aggregation can merge together two accumulators
     */
    boolean canMerge();

    /**
     * Generates (into the current statement list, gleaned by calling <code>
     * implementor</code>'s {@link
     * org.eigenbase.oj.rel.JavaRelImplementor#getStatementList} method) code to
     * merge two accumulators. For <code>sum(x)</code>, this looks like <code>
     * ((saffron.runtime.Holder.int_Holder) accumulator).value +=
     * ((saffron.runtime.Holder.int_Holder) other).value</code>.
     *
     * <p>The method is only called if {@link #canMerge} returns <code>
     * true</code>.</p>
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param otherAccumulator accumulator to merge in
     */
    void implementMerge(
        JavaRelImplementor implementor,
        RelNode rel,
        Expression accumulator,
        Expression otherAccumulator);

    /**
     * Generates (into the current statement list, gleaned by calling <code>
     * implementor</code>'s {@link
     * org.eigenbase.oj.rel.JavaRelImplementor#getStatementList} method) the
     * piece of code which gets called each time an extra row is seen. For
     * <code>sum(x)</code>, this looks like <code>
     * ((org.eigenbase.runtime.Holder.int_Holder) accumulator).value +=
     * x</code>.
     *
     * @param implementor a callback object which knows how to generate things
     * @param rel the relational expression which is generating this code
     * @param accumulator the expression which holds the total
     * @param call the ordinals of the fields of the child row which are
     * arguments to this aggregation
     */
    void implementNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression accumulator,
        AggregateCall call);

    /**
     * Generates the expression which gets called when a total is complete. For
     * <code>sum(x)</code>, this looks like <code>
     * ((saffron.runtime.Holder.int_Holder) accumulator).value</code>.
     */
    Expression implementResult(
        JavaRelImplementor implementor,
        Expression accumulator,
        AggregateCall call);
}

// End OJAggImplementor.java
