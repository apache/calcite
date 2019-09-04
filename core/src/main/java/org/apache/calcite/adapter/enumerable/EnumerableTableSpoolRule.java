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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableSpool;

/**
 * Rule to convert a {@link LogicalTableSpool} into an
 * {@link EnumerableTableSpool}.
 *
 * <p>NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
public class EnumerableTableSpoolRule extends ConverterRule {

  EnumerableTableSpoolRule() {
    super(LogicalTableSpool.class,
        Convention.NONE,
        EnumerableConvention.INSTANCE,
        "EnumerableTableSpoolRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalTableSpool spool = (LogicalTableSpool) rel;
    return EnumerableTableSpool.create(
        convert(spool.getInput(),
            spool.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE)),
        spool.readType,
        spool.writeType,
        spool.getTable());
  }
}

// End EnumerableTableSpoolRule.java
