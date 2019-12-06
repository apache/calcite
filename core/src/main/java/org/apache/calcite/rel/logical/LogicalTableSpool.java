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
package org.apache.calcite.rel.logical;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Sub-class of {@link TableSpool} not targeted at any particular engine or
 * calling convention.
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public class LogicalTableSpool extends TableSpool {

  //~ Constructors -----------------------------------------------------------
  public LogicalTableSpool(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      Type readType, Type writeType, RelOptTable table) {
    super(cluster, traitSet, input, readType, writeType, table);
  }

  /** Creates a LogicalTableSpool. */
  public static LogicalTableSpool create(RelNode input, Type readType,
      Type writeType, RelOptTable table) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> mq.collations(input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> mq.distribution(input));
    return new LogicalTableSpool(cluster, traitSet, input, readType, writeType, table);
  }

  //~ Methods ----------------------------------------------------------------

  @Override protected Spool copy(RelTraitSet traitSet, RelNode input,
      Type readType, Type writeType) {
    return new LogicalTableSpool(input.getCluster(), traitSet, input,
        readType, writeType, table);
  }
}

// End LogicalTableSpool.java
