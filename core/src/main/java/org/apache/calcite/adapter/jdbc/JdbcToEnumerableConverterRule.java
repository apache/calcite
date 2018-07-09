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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Predicates;

/**
 * Rule to convert a relational expression from
 * {@link JdbcConvention} to
 * {@link EnumerableConvention}.
 */
public class JdbcToEnumerableConverterRule extends ConverterRule {
  /** Creates a JdbcToEnumerableConverterRule. */
  public JdbcToEnumerableConverterRule(JdbcConvention out,
      RelBuilderFactory relBuilderFactory) {
    super(RelNode.class, Predicates.<RelNode>alwaysTrue(), out,
        EnumerableConvention.INSTANCE, relBuilderFactory,
        "JdbcToEnumerableConverterRule:" + out);
  }

  @Override public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    return new JdbcToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}

// End JdbcToEnumerableConverterRule.java
