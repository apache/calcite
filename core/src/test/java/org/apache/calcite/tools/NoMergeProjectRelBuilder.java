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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.test.RelOptRulesTest;

/**
 * Default {@link RelBuilder} always merges projects, so this class is helpful to craft
 * non-merged projects for testing purposes like {@link RelOptRulesTest#testOomProjectMergeRule()}.
 */
public class NoMergeProjectRelBuilder extends RelBuilder {
  private NoMergeProjectRelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public static RelBuilder create(FrameworkConfig config) {
    RelBuilder defaultBuilder = RelBuilder.create(config);
    return new NoMergeProjectRelBuilder(config.getContext(), defaultBuilder.cluster,
        defaultBuilder.relOptSchema);
  }

  @Override protected boolean shouldMergeProject() {
    return false;
  }
}

// End NoMergeProjectRelBuilder.java
