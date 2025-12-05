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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a table
 * in InnoDB data source.
 */
public class InnodbToEnumerableConverter extends ConverterImpl
    implements EnumerableRel {
  protected InnodbToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new InnodbToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return requireNonNull(super.computeSelfCost(planner, mq));
  }

  static List<String> innodbFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder list = new BlockBuilder();
    final InnodbRel.Implementor innodbImplementor = new InnodbRel.Implementor();
    innodbImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));
    final Expression fields =
        list.append("fields",
            constantArrayList(
                Pair.zip(InnodbToEnumerableConverter.innodbFieldNames(rowType),
                    new AbstractList<Class>() {
                      @Override public Class get(int index) {
                        return physType.fieldClass(index);
                      }

                      @Override public int size() {
                        return rowType.getFieldCount();
                      }
                    }),
                Pair.class));
    PairList<String, String> selectList =
        PairList.of(innodbImplementor.selectFields);
    final Expression selectFields =
        list.append("selectFields", constantArrayList(selectList, Pair.class));
    final Expression table =
        list.append("table",
            requireNonNull(
                innodbImplementor.table.getExpression(
                    InnodbTable.InnodbQueryable.class)));
    IndexCondition condition = innodbImplementor.indexCondition;
    final Expression indexName =
        list.append("indexName",
            Expressions.constant(condition.getIndexName(), String.class));
    final Expression queryType =
        list.append("queryType",
            Expressions.constant(condition.getQueryType(), QueryType.class));
    final Expression pointQueryKey =
        list.append("pointQueryKey",
            constantArrayList(condition.getPointQueryKey(), Object.class));
    final Expression rangeQueryLowerOp =
        list.append("rangeQueryLowerOp",
            Expressions.constant(condition.getRangeQueryLowerOp(), ComparisonOperator.class));
    final Expression rangeQueryLowerKey =
        list.append("rangeQueryLowerKey",
            constantArrayList(condition.getRangeQueryLowerKey(), Object.class));
    final Expression rangeQueryUpperOp =
        list.append("rangeQueryUpperOp",
            Expressions.constant(condition.getRangeQueryUpperOp(), ComparisonOperator.class));
    final Expression rangeQueryUpperKey =
        list.append("rangeQueryUpperKey",
            constantArrayList(condition.getRangeQueryUpperKey(), Object.class));
    final Expression cond =
        list.append("condition",
            Expressions.call(IndexCondition.class,
                "create", indexName, queryType, pointQueryKey,
                rangeQueryLowerOp, rangeQueryUpperOp, rangeQueryLowerKey,
                rangeQueryUpperKey));
    final Expression ascOrder =
        Expressions.constant(innodbImplementor.ascOrder);
    Expression enumerable =
        list.append("enumerable",
            Expressions.call(table,
                InnodbMethod.INNODB_QUERYABLE_QUERY.method, fields,
                selectFields, cond, ascOrder));
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("Innodb: " + Expressions.toString(enumerable));
    }
    list.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /**
   * E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')".
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <T> Expression constantArrayList(List<T> values, Class clazz) {
    if (values instanceof PairList
        && !values.isEmpty()
        && Map.Entry.class.isAssignableFrom(clazz)) {
      // For PairList, we cannot generate Arrays.asList because Map.Entry does
      // not an obvious implementation with default constructor. Instead,
      // generate
      //   PairList.of("k0", "v0", "k1", "v1");
      final List<Object> keyValues =
          ((PairList<Object, Object>) values).stream()
              .flatMap(p -> Stream.of(p.getKey(), p.getValue()))
              .collect(Collectors.toList());
      return Expressions.call(null, BuiltInMethod.PAIR_LIST_COPY_OF.method,
          constantList(keyValues));
    }
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /**
   * E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}.
   */
  private static <T> List<Expression> constantList(List<T> values) {
    if (values.isEmpty()) {
      return Collections.emptyList();
    }
    return Util.transform(values, Expressions::constant);
  }
}
