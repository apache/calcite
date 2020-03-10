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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;

/** For backwards compatibility.
 *
 * @deprecated Use equivalent fields in {@link MaterializedViewRules}.
 */
public abstract class AbstractMaterializedViewRule {
  private AbstractMaterializedViewRule() {
  }

  public static final RelOptRule INSTANCE_PROJECT_AGGREGATE =
      MaterializedViewRules.PROJECT_AGGREGATE;

  public static final RelOptRule INSTANCE_AGGREGATE =
      MaterializedViewRules.AGGREGATE;

  public static final RelOptRule INSTANCE_FILTER =
      MaterializedViewRules.FILTER;

  public static final RelOptRule INSTANCE_JOIN =
      MaterializedViewRules.JOIN;

  public static final RelOptRule INSTANCE_PROJECT_FILTER =
      MaterializedViewRules.PROJECT_FILTER;

  public static final RelOptRule INSTANCE_PROJECT_JOIN =
      MaterializedViewRules.PROJECT_JOIN;
}
