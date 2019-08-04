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
 * <pre>
 *   select &#47;&#42;&#43; NO_HASH_JOIN, RESOURCE(mem='128mb', parallelism='24') &#42;&#47;
 *   from
 *     emp &#47;&#42;&#43; INDEX(idx1, idx2) &#42;&#47;
 *     join
 *     dept &#47;&#42;&#43; PROPERTIES(k1='v1', k2='v2') &#42;&#47;
 *     on emp.deptno=dept.deptno
 * </pre>
 *
 * <h2>Customize Hint Matching Rules</h2>
 * Calcite implements a framework to define and propagate the hints. In order to make the hints
 *  propagate efficiently, every hint referenced in the sql statement needs to
 *  register the propagation rules.
 *
 * <p>We support two kinds of matching rules for rule registration:
 *
 * <ol>
 *   <li>One is {@link org.apache.calcite.rel.hint.NodeTypeHintStrategy}, this rule matches a
 *   relational expression by the relational node type.</li>
 *   <li>Another is {@link org.apache.calcite.rel.hint.ExplicitHintStrategy},
 *   this rule's matching logic can be totally customized.</li>
 * </ol>
 *
 * <p>Here is the code snippet to illustrate how to config the strategies:
 *
 * <pre>
 *       // Initialize a HintStrategyTable.
 *       HintStrategyTable strategies = HintStrategyTable.builder()
 *         .addHintStrategy("time_zone", HintStrategies.SET_VAR)
 *         .addHintStrategy("index", HintStrategies.TABLE_SCAN)
 *         .addHintStrategy("resource", HintStrategies.PROJECT)
 *         .addHintStrategy("use_hash_join",
 *             HintStrategies.cascade(HintStrategies.JOIN,
 *                 HintStrategies.explicit((hint, rel) -&gt; {
 *                   ...
 *                 })))
 *         .build();
 *      // Config the strategies in the config.
 *      SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
 *          .withHintStrategyTable(strategies)
 *          .build();
 *      // Use the config to initialize the SqlToRelConverter.
 *   ...
 * </pre>
 *
 * <h2>Hints Propagation</h2>
 * There are two cases we need to consider the hints propagation:
 *
 * <ul>
 *   <li>Right after a {@code SqlNode} was converted to {@code RelNode}, we would
 *   propagate the hints from the attaching to its input(children) nodes. The hints was
 *   propagated recursively with a {@code RelShuttle}, see
 *   SqlToRelConverter#RelHintPropagateShuttle for how it works.</li>
 *   <li>During rule planning, in the transforming phrase of a {@code RelOptRule},
 *   you should copy the hints by hand if it is needed;
 *   We make some effort to make the thing easier: right before the new relational expression
 *   was registered into the planner, the hints of the old relational expression was
 *   copied into the new expression if both of them all implement
 *   {@link org.apache.calcite.rel.hint.Hintable}.</li>
 * </ul>
 *
 * <h2>Caution</h2>
 * We do not support hints propagation with decorrelation(the hints would all be dropped).
 *
 * <h2>Design Doc</h2>
 * <a href="https://docs.google.com/document/d/1mykz-w2t1Yw7CH6NjUWpWqCAf_6YNKxSc59gXafrNCs/edit?usp=sharing">Calcite SQL and Planner Hints Design</a>.
 */
@PackageMarker
package org.apache.calcite.rel.hint;

import org.apache.calcite.avatica.util.PackageMarker;

// End package-info.java
