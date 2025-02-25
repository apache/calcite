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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.Exists;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that translates a {@link Intersect}
 * (<code>all</code> = <code>false</code>)
 * into a {@link Exists}.
 *
 * @see CoreRules#INTERSECT_TO_EXISTS
 */
@Value.Enclosing
public class IntersectToExistsRule
    extends RelRule<IntersectToExistsRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToExistRule. */
  protected IntersectToExistsRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public IntersectToExistsRule(Class<? extends Intersect> intersectClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(intersectClass));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    if (intersect.all) {
      return; // nothing we can do
    }

    final RelBuilder builder = call.builder();
    final RexBuilder rexBuilder = builder.getRexBuilder();

    // get all column indices of intersect
    List<Integer> fieldIndices = intersect.getRowType().getFieldList()
        .stream().map(RelDataTypeField::getIndex)
        .collect(Collectors.toList());

    List<RelNode> inputs = intersect.getInputs();
    RelNode current = inputs.get(0);

    // iterate over the inputs and apply exists subquery
    for (int i = 1; i < inputs.size(); i++) {
      RelNode nextInput = inputs.get(i);

      // create correlation
      CorrelationId correlationId = intersect.getCluster().createCorrel();
      RexNode correl =
          rexBuilder.makeCorrel(nextInput.getRowType(), correlationId);

      // create condition in exists filter, and use correlation
      List<RexNode> conditions = new ArrayList<>();
      for (int fieldIndex : fieldIndices) {
        RexNode outerField = rexBuilder.makeInputRef(current, fieldIndex);
        RexNode innerField =
            rexBuilder.makeFieldAccess(correl, fieldIndex);
        conditions.add(
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                outerField, innerField));
      }
      RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions);

      // build exists subquery
      RelNode existsSubQuery = builder.push(nextInput)
          .filter(condition)
          .project(builder.fields(fieldIndices))
          .build();

      // apply exists subquery to the current relation
      current = builder.push(current)
          .filter(ImmutableSet.of(correlationId),
              RexSubQuery.exists(existsSubQuery))
          .build();
    }
    System.out.println(RelOptUtil.toString(current));
    RelNode result = builder.push(current)
        .project(builder.fields(fieldIndices))
        .distinct()
        .build();
    System.out.println(result);
    call.transformTo(result);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectToExistsRule.Config.of()
        .withOperandFor(LogicalIntersect.class);

    @Override default IntersectToExistsRule toRule() {
      return new IntersectToExistsRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }
  }
}
