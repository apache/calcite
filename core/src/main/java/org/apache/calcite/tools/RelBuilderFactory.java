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
package org.apache.calcite.tools;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.core.RelFactories;

/** A partially-created RelBuilder.
 *
 * <p>Add a cluster, and optionally a schema,
 * when you want to create a builder.
 *
 * <p>A {@code ProtoRelBuilder} can be shared among queries, and thus can
 * be inside a {@link RelOptRule}. It is a nice way to encapsulate the policy
 * that this particular rule instance should create {@code DrillFilter}
 * and {@code DrillProject} versus {@code HiveFilter} and {@code HiveProject}.
 *
 * @see RelFactories#LOGICAL_BUILDER
 */
public interface RelBuilderFactory {
  /** Creates a RelBuilder. */
  RelBuilder create(RelOptCluster cluster, RelOptSchema schema);
}

// End RelBuilderFactory.java
