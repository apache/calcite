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
import org.apache.calcite.rel.logical.LogicalMatch;

/**
 * Rule to convert a {@link LogicalMatch} to an
 * {@link EnumerableMatch}.
 */
public class EnumerableMatchRule extends ConverterRule {
  EnumerableMatchRule() {
    super(LogicalMatch.class, Convention.NONE, EnumerableConvention.INSTANCE,
        "EnumerableMatchRule");
  }

  @Override public RelNode convert(RelNode rel) {
    final LogicalMatch match = (LogicalMatch) rel;
    return EnumerableMatch.create(match.getInput(), match.getRowType(),
        match.getPattern(), match.isStrictStart(), match.isStrictEnd(),
        match.getPatternDefinitions(), match.getMeasures(), match.getAfter(),
        match.getSubsets(), match.isAllRows(), match.getPartitionKeys(),
        match.getOrderKeys(), match.getInterval());
  }
}

// End EnumerableMatchRule.java
