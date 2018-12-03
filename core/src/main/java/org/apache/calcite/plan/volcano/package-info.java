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

/**
 * Optimizes relational expressions.<p>&nbsp;</p>
 *
 * <h2>Overview</h2>
 *
 * <p>A <dfn>planner</dfn> (also known as an <dfn>optimizer</dfn>) finds the
 * most efficient implementation of a
 * {@link org.apache.calcite.rel.RelNode relational expression}.</p>
 *
 * <p>Interface {@link org.apache.calcite.plan.RelOptPlanner} defines a planner,
 * and class {@link org.apache.calcite.plan.volcano.VolcanoPlanner} is an
 * implementation which uses a dynamic programming technique. It is based upon
 * the Volcano optimizer [<a href="#graefe93">1</a>].</p>
 *
 * <p>Interface {@link org.apache.calcite.plan.RelOptCost} defines a cost
 * model; class {@link org.apache.calcite.plan.volcano.VolcanoCost} is
 * the implementation for a <code>VolcanoPlanner</code>.</p>
 *
 * <p>A {@link org.apache.calcite.plan.volcano.RelSet} is a set of equivalent
 * relational expressions.  They are equivalent because they will produce the
 * same result for any set of input data. It is an equivalence class: two
 * expressions are in the same set if and only if they are in the same
 * <code>RelSet</code>.</p>
 *
 * <p>One of the unique features of the optimizer is that expressions can take
 * on a variety of physical traits. Each relational expression has a set of
 * traits. Each trait is described by an implementation of
 * {@link org.apache.calcite.plan.RelTraitDef}.  Manifestations of the trait
 * implement {@link org.apache.calcite.plan.RelTrait}. The most common example
 * of a trait is calling convention: the protocol used to receive and transmit
 * data. {@link org.apache.calcite.plan.ConventionTraitDef} defines the trait
 * and {@link org.apache.calcite.plan.Convention} enumerates the
 * protocols. Every relational expression has a single calling convention by
 * which it returns its results. Some examples:</p>
 *
 * <ul>
 *     <li>{@link org.apache.calcite.adapter.jdbc.JdbcConvention} is a fairly
 *         conventional convention; the results are rows from a
 *         {@link java.sql.ResultSet JDBC result set}.
 *     </li>
 *     <li>{@link org.apache.calcite.plan.Convention#NONE} means that a
 *         relational
 *         expression cannot be implemented; typically there are rules which can
 *         transform it to equivalent, implementable expressions.
 *     </li>
 *     <li>{@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
 *         implements the expression by
 *         generating Java code. The code places the current row in a Java
 *         variable, then
 *         calls the piece of code which implements the consuming relational
 *         expression.
 *         For example, a Java array reader of the <code>names</code> array
 *         would generate the following code:
 *         <blockquote>
 *     <pre>String[] names;
 * for (int i = 0; i &lt; names.length; i++) {
 *     String name = names[i];
 *     // ... code for consuming relational expression ...
 * }</pre>
 *         </blockquote>
 *     </li>
 * </ul>
 *
 * <p>New traits are added to the planner in one of two ways:</p>
 * <ol>
 * <li>If the new trait is integral to Calcite, then each and every
 *     implementation of {@link org.apache.calcite.rel.RelNode} should include
 *     its manifestation of the trait as part of the
 *     {@link org.apache.calcite.plan.RelTraitSet} passed to
 *     {@link org.apache.calcite.rel.AbstractRelNode}'s constructor. It may be
 *     useful to provide alternate <code>AbstractRelNode</code> constructors
 *     if most relational expressions use a single manifestation of the
 *     trait.</li>
 *
 * <li>If the new trait describes some aspect of a Farrago extension, then
 *     the RelNodes passed to
 *     {@link org.apache.calcite.plan.volcano.VolcanoPlanner#setRoot(org.apache.calcite.rel.RelNode)}
 *     should have their trait sets expanded before the
 *     <code>setRoot(RelNode)</code> call.</li>
 *
 * </ol>
 *
 * <p>The second trait extension mechanism requires that implementations of
 * {@link org.apache.calcite.rel.AbstractRelNode#clone()} must not assume the
 * type and quantity of traits in their trait set. In either case, the new
 * <code>RelTraitDef</code> implementation must be
 * {@link org.apache.calcite.plan.volcano.VolcanoPlanner#addRelTraitDef(org.apache.calcite.plan.RelTraitDef)}
 * registered with the planner.</p>
 *
 * <p>A {@link org.apache.calcite.plan.volcano.RelSubset} is a subset of a
 * <code>RelSet</code> containing expressions which are equivalent and which
 * have the same <code>Convention</code>. Like <code>RelSet</code>, it is an
 * equivalence class.</p>
 *
 * <h2>Related packages</h2>
 * <ul>
 * <li>{@code <a href="../rel/package-summary.html">org.apache.calcite.rel</a>}
 *     defines {@link org.apache.calcite.rel.RelNode relational expressions}.
 * </li>
 * </ul>
 *
 * <h2>Details</h2>
 *
 * <p>...</p>
 *
 * <p>Sets merge when the result of a rule already exists in another set. This
 *     implies that all of the expressions are equivalent. The RelSets are
 *     merged, and so are the contained RelSubsets.</p>
 *
 * <p>Expression registration.</p>
 * <ul>
 *     <li>Expression is added to a set. We may find that an equivalent
 *         expression already exists. Otherwise, this is the moment when an
 *         expression becomes public, and fixed. Its digest is assigned, which
 *         allows us to quickly find identical expressions.</li>
 *
 *     <li>We match operands, figure out which rules are applicable, and
 *         generate rule calls. The rule calls are placed on a queue, and the
 *         important ones are called later.</li>
 *
 *     <li>RuleCalls allow us to defer the invocation of rules. When an
 *         expression is registered </li>
 * </ul>
 *
 * <p>Algorithm</p>
 *
 * <p>To optimize a relational expression R:</p>
 *
 * <p>1. Register R.</p>
 *
 * <p>2. Create rule-calls for all applicable rules.</p>
 *
 * <p>3. Rank the rule calls by importance.</p>
 *
 * <p>4. Call the most important rule</p>
 *
 * <p>5. Repeat.</p>
 *
 * <p><b>Importance</b>. A rule-call is important if it is likely to produce
 *     better implementation of a relexp on the plan's critical path. Hence (a)
 *     it produces a member of an important RelSubset, (b) its children are
 *     cheap.</p>
 *
 * <p>Conversion. Conversions are difficult because we have to work backwards
 *     from the goal.</p>
 *
 * <p><b>Rule triggering</b></p>
 *
 * <p>The rules are:</p>
 * <ol>
 *     <li><code>PushFilterThroughProjectRule</code>. Operands:
 *         <blockquote>
 *     <pre>Filter
 *   Project</pre>
 *         </blockquote>
 *     </li>
 *     <li><code>CombineProjectsRule</code>. Operands:
 *         <blockquote>
 *     <pre>Project
 *   Project</pre>
 *         </blockquote>
 *     </li>
 * </ol>
 *
 * <p>A rule can be triggered by a change to any of its operands. Consider the
 *     rule to combine two filters into one. It would have operands [Filter
 *     [Filter]].  If I register a new Filter, it will trigger the rule in 2
 *     places. Consider:</p>
 *
 * <blockquote>
 *   <pre>Project (deptno)                              [exp 1, subset A]
 *   Filter (gender='F')                         [exp 2, subset B]
 *     Project (deptno, gender, empno)           [exp 3, subset C]
 *       Project (deptno, gender, empno, salary) [exp 4, subset D]
 *         TableScan (emp)                       [exp 0, subset X]</pre>
 * </blockquote>
 * <p>Apply <code>PushFilterThroughProjectRule</code> to [exp 2, exp 3]:</p>
 * <blockquote>
 *   <pre>Project (deptno)                              [exp 1, subset A]
 *   Project (deptno, gender, empno)             [exp 5, subset B]
 *     Filter (gender='F')                       [exp 6, subset E]
 *       Project (deptno, gender, empno, salary) [exp 4, subset D]
 *         TableScan (emp)                       [exp 0, subset X]</pre>
 * </blockquote>
 *
 * <p>Two new expressions are created. Expression 5 is in subset B (because it
 *     is equivalent to expression 2), and expression 6 is in a new equivalence
 *     class, subset E.</p>
 *
 * <p>The products of a applying a rule can trigger a cascade of rules. Even in
 *     this simple system (2 rules and 4 initial expressions), two more rules
 *     are triggered:</p>
 *
 * <ul>
 *
 *     <li>Registering exp 5 triggers <code>CombineProjectsRule</code>(exp 1,
 *         exp 5), which creates
 *
 *         <blockquote>
 *     <pre>Project (deptno)                              [exp 7, subset A]
 *   Filter (gender='F')                         [exp 6, subset E]
 *     Project (deptno, gender, empno, salary)   [exp 4, subset D]
 *       TableScan (emp)                         [exp 0, subset X]</pre>
 *         </blockquote>
 *     </li>
 *
 *     <li>Registering exp 6 triggers
 *         <code>PushFilterThroughProjectRule</code>(exp 6, exp 4), which
 *         creates
 *
 *         <blockquote>
 *     <pre>Project (deptno)                              [exp 1, subset A]
 *   Project (deptno, gender, empno)             [exp 5, subset B]
 *     Project (deptno, gender, empno, salary)   [exp 8, subset E]
 *       Filter (gender='F')                     [exp 9, subset F]
 *         TableScan (emp)                       [exp 0, subset X]</pre>
 *         </blockquote>
 *     </li>
 * </ul>
 *
 * <p>Each rule application adds additional members to existing subsets. The
 *     non-singleton subsets are now A {1, 7}, B {2, 5} and E {6, 8}, and new
 *     combinations are possible. For example,
 *     <code>CombineProjectsRule</code>(exp 7, exp 8) further reduces the depth
 *     of the tree to:</p>
 *
 * <blockquote>
 *   <pre>Project (deptno)                          [exp 10, subset A]
 *   Filter (gender='F')                     [exp 9, subset F]
 *     TableScan (emp)                       [exp 0, subset X]</pre>
 * </blockquote>
 * <p>Todo: show how rules can cause subsets to merge.</p>
 *
 * <p>Conclusion:</p>
 * <ol>
 *     <li>A rule can be triggered by any of its operands.</li>
 *     <li>If a subset is a child of more than one parent, it can trigger rule
 *         matches for any of its parents.
 *     </li>
 *
 *     <li>Registering one relexp can trigger several rules (and even the same
 *         rule several times).</li>
 *
 *     <li>Firing rules can cause subsets to merge.</li>
 * </ol>
 * <h2>References</h2>
 *
 * <p>1. <a id="graefe93" href="http://citeseer.nj.nec.com/graefe93volcano.html">The
 *     Volcano Optimizer
 *     Generator: Extensibility and Efficient Search - Goetz Graefe, William J.
 *     McKenna
 *     (1993)</a>.</p>
 */
@PackageMarker
package org.apache.calcite.plan.volcano;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
