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
package org.apache.calcite.rel.stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableScan;

/**
 * Relational operator that converts a relation to a stream.
 *
 * <p>For example, if {@code Orders} is a table, and {@link TableScan}(Orders)
 * is a relational operator that returns the current contents of the table,
 * then {@link Delta}(TableScan(Orders)) is a relational operator that returns
 * all inserts into the table.
 *
 * <p>If unrestricted, Delta returns all previous inserts into the table (from
 * time -&infin; to now) and all future inserts into the table (from now
 * to +&infin;) and never terminates.
 */
public abstract class Delta extends SingleRel {
  protected Delta(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }

  /** Creates a Delta by parsing serialized output. */
  protected Delta(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput());
  }
}

// End Delta.java
