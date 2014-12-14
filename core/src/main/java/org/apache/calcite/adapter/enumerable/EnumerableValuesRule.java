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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalValues}
 * relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableValuesRule extends ConverterRule {
  EnumerableValuesRule() {
    super(LogicalValues.class, Convention.NONE, EnumerableConvention.INSTANCE,
        "EnumerableValuesRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalValues values = (LogicalValues) rel;
    return EnumerableValues.create(values.getCluster(), values.getRowType(),
        values.getTuples());
  }
}

// End EnumerableValuesRule.java
