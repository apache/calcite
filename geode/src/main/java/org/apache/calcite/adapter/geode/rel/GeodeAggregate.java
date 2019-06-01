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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of
 * {@link org.apache.calcite.rel.core.Aggregate} relational expression
 * in Geode.
 */
public class GeodeAggregate extends Aggregate implements GeodeRel {

  /** Creates a GeodeAggregate. */
  public GeodeAggregate(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, input, groupSet, groupSets, aggCalls);

    assert getConvention() == GeodeRel.CONVENTION;
    assert getConvention() == this.input.getConvention();
    assert getConvention() == input.getConvention();
    assert this.groupSets.size() == 1 : "Grouping sets not supported";

    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        System.out.println("DISTINCT based aggregation!");
      }
    }
  }

  @Deprecated // to be removed before 2.0
  public GeodeAggregate(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    this(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    checkIndicator(indicator);
  }

  @Override public Aggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new GeodeAggregate(getCluster(), traitSet, input, groupSet,
        groupSets, aggCalls);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(GeodeImplementContext geodeImplementContext) {
    geodeImplementContext.visitChild(getInput());

    List<String> inputFields = fieldNames(getInput().getRowType());

    List<String> groupByFields = new ArrayList<>();

    for (int group : groupSet) {
      groupByFields.add(inputFields.get(group));
    }

    geodeImplementContext.addGroupBy(groupByFields);

    // Find the aggregate functions (e.g. MAX, SUM ...)
    Builder<String, String> aggregateFunctionMap = ImmutableMap.builder();
    for (AggregateCall aggCall : aggCalls) {

      List<String> aggCallFieldNames = new ArrayList<>();
      for (int i : aggCall.getArgList()) {
        aggCallFieldNames.add(inputFields.get(i));
      }
      String functionName = aggCall.getAggregation().getName();

      // Workaround to handle count(*) case. Geode doesn't allow "AS" aliases on
      // 'count(*)' but allows it for count('any column name'). So we are
      // converting the count(*) into count (first input ColumnName).
      if ("COUNT".equalsIgnoreCase(functionName) && aggCallFieldNames.isEmpty()) {
        aggCallFieldNames.add(inputFields.get(0));
      }

      String oqlAggregateCall = Util.toString(aggCallFieldNames, functionName + "(", ", ",
          ")");

      aggregateFunctionMap.put(aggCall.getName(), oqlAggregateCall);
    }

    geodeImplementContext.addAggregateFunctions(aggregateFunctionMap.build());

  }

  private List<String> fieldNames(RelDataType relDataType) {
    ArrayList<String> names = new ArrayList<>();

    for (RelDataTypeField rdtf : relDataType.getFieldList()) {
      names.add(rdtf.getName());
    }
    return names;
  }
}

// End GeodeAggregate.java
