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
package org.apache.calcite.interpreter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicates;

/**
 * Rule to convert a relational expression from
 * {@link org.apache.calcite.plan.Convention#NONE}
 * to {@link org.apache.calcite.interpreter.BindableConvention}.
 */
public class NoneToBindableConverterRule extends ConverterRule {
  public static final ConverterRule INSTANCE =
      new NoneToBindableConverterRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates a NoneToBindableConverterRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public NoneToBindableConverterRule(RelBuilderFactory relBuilderFactory) {
    super(RelNode.class, Predicates.<RelNode>alwaysTrue(), Convention.NONE,
        BindableConvention.INSTANCE, relBuilderFactory,
        "NoneToBindableConverterRule");
  }

  @Override public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new InterpretableConverter(rel.getCluster(), newTraitSet, rel);
  }
}

// End NoneToBindableConverterRule.java
