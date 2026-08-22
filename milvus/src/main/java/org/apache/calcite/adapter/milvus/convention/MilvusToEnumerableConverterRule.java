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
package org.apache.calcite.adapter.milvus.convention;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link MilvusRel#CONVENTION Milvus calling convention} to
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention Enumerable calling convention}.
 */
public class MilvusToEnumerableConverterRule extends ConverterRule {
  public static final MilvusToEnumerableConverterRule INSTANCE =
      Config.INSTANCE
          .withConversion(MilvusRel.class, MilvusRel.CONVENTION,
              EnumerableConvention.INSTANCE, "MilvusToEnumerableConverterRule")
          .withRuleFactory(MilvusToEnumerableConverterRule::new)
          .toRule(MilvusToEnumerableConverterRule.class);

  protected MilvusToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode relNode) {
    final RelTraitSet traitSet = relNode.getTraitSet()
        .replace(getOutTrait());
    return new MilvusToEnumerableConverter(relNode.getCluster(), traitSet,
        relNode);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final RelNode rel = call.rel(0);
    if (rel.getConvention() == getOutTrait()) {
      return;
    }
    final RelNode converted = convert(rel);
    if (converted != null) {
      call.transformTo(converted);
    }
  }
}
