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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Combine;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Combine} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableCombine extends Combine implements EnumerableRel {
  public EnumerableCombine(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelNode> inputs) {
    super(cluster, traitSet, inputs);
  }

  @Override public EnumerableCombine copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableCombine(getCluster(), traitSet, inputs);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final RelDataType rowType = getRowType();

    // Build a list of rows, one per input query
    // Each row is Object[]{queryIdx, resultsList}
    final List<Expression> rowExpressions = new ArrayList<>();

    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      EnumerableRel input = (EnumerableRel) ord.e;
      final Result result = implementor.visitChild(this, ord.i, input, pref);
      Expression childExp =
          builder.append(
              "child" + ord.i,
              result.block);

      // Get column names for this input
      final List<RelDataTypeField> fields = input.getRowType().getFieldList();
      final int fieldCount = fields.size();

      // Transform each row to a Map with column names as keys
      // For single column: value -> {colName: value}
      // For multi-column: Object[] -> {col1: val1, col2: val2, ...}
      ParameterExpression row = Expressions.parameter(Object.class, "row" + ord.i);

      // Build the arguments for SqlFunctions.map(key1, val1, key2, val2, ...)
      List<Expression> mapArgs = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        String colName = fields.get(i).getName();
        mapArgs.add(Expressions.constant(colName));
        if (fieldCount > 1) {
          // Multi-column: access row[i]
          mapArgs.add(
              Expressions.arrayIndex(
                  Expressions.convert_(row, Object[].class),
                  Expressions.constant(i)));
        } else {
          // Single column: use row directly
          mapArgs.add(row);
        }
      }

      Expression mapCall =
          Expressions.call(
              SqlFunctions.class,
              "map",
              Expressions.newArrayInit(Object.class, mapArgs));

      Expression selectLambda = Expressions.lambda(mapCall, row);
      Expression enumerableToConvert =
          builder.append("converted" + ord.i,
              Expressions.call(
                  childExp,
                  BuiltInMethod.SELECT.method,
                  selectLambda));

      // Convert Enumerable to List for the QUERY array
      Expression listExp =
          builder.append(
              "list" + ord.i,
              Expressions.call(
                  enumerableToConvert,
                  Types.lookupMethod(
                      Enumerable.class,
                      "toList")));

      // Each row is just the results list (single column)
      rowExpressions.add(listExp);
    }

    // The physical type represents the row structure: single QUERY column
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            rowType,
            pref.prefer(JavaRowFormat.SCALAR));

    // Create an enumerable from the list of rows (each row is a single List value)
    Expression rowListExp =
        Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(Object.class, rowExpressions));

    Expression enumerableExp =
        Expressions.call(
            Types.lookupMethod(
                Linq4j.class,
                "asEnumerable",
                List.class),
            rowListExp);

    builder.add(enumerableExp);

    return implementor.result(physType, builder.toBlock());
  }
}
