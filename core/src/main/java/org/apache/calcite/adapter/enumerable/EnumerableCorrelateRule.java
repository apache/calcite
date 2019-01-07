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
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Implementation of nested loops over enumerable inputs.
 */
public class EnumerableCorrelateRule extends ConverterRule {
  /**
   * Creates an EnumerableCorrelateRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public EnumerableCorrelateRule(RelBuilderFactory relBuilderFactory) {
    super(LogicalCorrelate.class, (Predicate<RelNode>) r -> true,
        Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory,
        "EnumerableCorrelateRule");
  }

  public RelNode convert(RelNode rel) {
    final LogicalCorrelate c = (LogicalCorrelate) rel;
    return EnumerableCorrelate.create(
        convert(c.getLeft(), c.getLeft().getTraitSet()
            .replace(EnumerableConvention.INSTANCE)),
        convert(c.getRight(), c.getRight().getTraitSet()
            .replace(EnumerableConvention.INSTANCE)),
        c.getCorrelationId(),
        c.getRequiredColumns(),
        c.getJoinType());
  }
}

// End EnumerableCorrelateRule.java
