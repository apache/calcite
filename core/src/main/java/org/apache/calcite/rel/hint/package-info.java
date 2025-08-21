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
 * Defines hints interfaces and utilities for relational expressions.
 *
 * <h2>The Syntax</h2>
 * We support the Oracle style hint grammar for both query hint(right after the "SELECT" keyword)
 * and the table hint(right after the table name reference). i.e.
 *
 * <blockquote><pre>
 *   select &#47;&#42;&#43; NO_HASH_JOIN, RESOURCE(mem='128mb', parallelism='24') &#42;&#47;
 *   from
 *     emp &#47;&#42;&#43; INDEX(idx1, idx2) &#42;&#47;
 *     join
 *     dept &#47;&#42;&#43; PROPERTIES(k1='v1', k2='v2') &#42;&#47;
 *     on emp.deptno=dept.deptno
 * </pre></blockquote>
 *
 * <h2>Customize Hint Match Rules</h2>
 * Calcite implements a framework to define and propagate the hints. In order to make the hints
 * propagate efficiently, every hint referenced in the sql statement needs to
 * register the match rules for hints propagation.
 *
 * <p>A match rule is defined though {@link org.apache.calcite.rel.hint.HintPredicate}.
 * {@link org.apache.calcite.rel.hint.NodeTypeHintPredicate} matches a relational expression
 * by its node type; you can also define a custom instance with more complicated rules,
 * i.e. JOIN with specified relations from the hint options.
 *
 * <p>Here is the code snippet to illustrate how to config the strategies:
 *
 * <pre>
 *       // Initialize a HintStrategyTable.
 *       HintStrategyTable strategies = HintStrategyTable.builder()
 *         .hintStrategy("time_zone", HintPredicates.SET_VAR)
 *         .hintStrategy("index", HintPredicates.TABLE_SCAN)
 *         .hintStrategy("resource", HintPredicates.PROJECT)
 *         .hintStrategy("use_hash_join", HintPredicates.and(HintPredicates.JOIN))
 *         .hintStrategy("use_merge_join",
 *             HintStrategy.builder(
 *                 HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
 *                 .excludedRules(EnumerableRules.ENUMERABLE_JOIN_RULE).build())
 *         .build();
 *       // Config the strategies in the config.
 *       SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
 *         .withHintStrategyTable(strategies)
 *         .build();
 *       // Use the config to initialize the SqlToRelConverter.
 *   ...
 * </pre>
 *
 * <h2>Hints Propagation</h2>
 * There are two cases that need to consider the hints propagation:
 *
 * <ul>
 *   <li>Right after a {@code SqlNode} tree is converted to {@code RelNode} tree, we would
 *   propagate the hints from the attaching node to its input(children) nodes. The hints are
 *   propagated recursively with a {@code RelShuttle}, see
 *   RelOptUtil#RelHintPropagateShuttle for how it works.</li>
 *   <li>During rule planning, in the transforming phrase of a {@code RelOptRule},
 *   you <strong>should not</strong> copy the hints by hand. To ensure correctness,
 *   the hints copy work within planner rule is taken care of by Calcite;
 *   We make some effort to make the thing easier: right before the new relational expression
 *   was registered into the planner, the hints of the old relational expression was
 *   copied into the new expression sub-tree(by "new" we mean, the node was created
 *   just in the planner rule) if the nodes implement
 *   {@link org.apache.calcite.rel.hint.Hintable}.</li>
 * </ul>
 *
 * <h2>Design Doc</h2>
 * <a href="https://docs.google.com/document/d/1mykz-w2t1Yw7CH6NjUWpWqCAf_6YNKxSc59gXafrNCs/edit?usp=sharing">Calcite SQL and Planner Hints Design</a>.
 */
package org.apache.calcite.rel.hint;
