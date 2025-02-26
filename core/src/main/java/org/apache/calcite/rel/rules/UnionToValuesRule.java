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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that converts
 * {@link org.apache.calcite.rel.core.Union} with
 * inputs only containing {@link org.apache.calcite.rel.core.Values}
 * into an {@link org.apache.calcite.rel.core.Values}.
 *
 * <p>For example,
 *
 * <blockquote><pre>{@code
 * Union(all=[true])
 *     Values(tuples=[[{ 3, null }]])
 *     Values(tuples=[[{ 7369, null }]])
 *     Values(tuples=[[{ 1, 2 }]])
 * }</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote><pre>{@code
 * Values(tuples=[[{ 3, null }, { 7369, null }, { 1, 2 }]])
 * }</pre></blockquote>
 *
 * <p>If (<code>all</code> = <code>false</code>),
 * {@link org.apache.calcite.rel.core.Values} value will be set list.
 *
 */
@Value.Enclosing
public class UnionToValuesRule extends RelRule<UnionToValuesRule.Config>
    implements TransformationRule {

  /**
   * Creates a UnionToValuesRule.
   */
  protected UnionToValuesRule(UnionToValuesRule.Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    List<RelNode> relNodeList = union.getInputs();
    final RelDataType relDataType = union.getRowType();
    if (!relNodeList.stream().allMatch(relNode ->
        isValuesWithSpecifiedDataType(relNode, relDataType))) {
      return;
    }
    ImmutableCollection.Builder<ImmutableList<RexLiteral>> tupleList =
        getImmutableListBuilder(union.all, relNodeList);
    Values values =
        (Values) call.builder().values(tupleList.build(), union.getRowType()).build();
    call.transformTo(values);
  }

  private static ImmutableCollection.Builder<ImmutableList<RexLiteral>> getImmutableListBuilder(
      Boolean all, List<RelNode> relNodeList) {
    ImmutableCollection.Builder<ImmutableList<RexLiteral>> tupleList;
    if (all) {
      tupleList = ImmutableList.builder();
    } else {
      tupleList = ImmutableSet.builder();
    }
    for (RelNode relNode : relNodeList) {
      Values values = (Values) relNode.stripped();
      ImmutableList<ImmutableList<RexLiteral>> immutableLists = values.getTuples();
      for (ImmutableList<RexLiteral> immutableList : immutableLists) {
        ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
        for (RexLiteral rexLiteral : immutableList) {
          tuple.add(rexLiteral);
        }
        tupleList.add(tuple.build());
      }
    }
    return tupleList;
  }

  private static boolean isValuesWithSpecifiedDataType(RelNode node, RelDataType relDataType) {
    if (node instanceof Values
        && relDataType.equalsSansFieldNamesAndNullability(node.getRowType())) {
      return true;
    }
    if (node instanceof HepRelVertex || node instanceof RelSubset) {
      return isValuesWithSpecifiedDataType(node.stripped(), relDataType);
    }
    return false;
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableUnionToValuesRule.Config.of()
        .withDescription("UnionToValuesRule")
        .withOperandFor(Union.class);

    @Override default UnionToValuesRule toRule() {
      return new UnionToValuesRule(this);
    }

    /**
     * Defines an operand tree for the given classes.
     */
    default UnionToValuesRule.Config withOperandFor(Class<? extends Union> unionClass) {
      return withOperandSupplier(b0 ->
          b0.operand(unionClass).anyInputs())
          .as(UnionToValuesRule.Config.class);
    }
  }
}
