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
package org.apache.calcite.adapter.spark;

import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a JDBC data source
 * that returns its results as a Spark RDD.
 */
public class JdbcToSparkConverter
    extends ConverterImpl
    implements SparkRel {
  protected JdbcToSparkConverter(RelOptCluster cluster, RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new JdbcToSparkConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  public SparkRel.Result implementSpark(SparkRel.Implementor implementor) {
    // Generate:
    //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
    final BlockBuilder list = new BlockBuilder();
    final JdbcRel child = (JdbcRel) getInput();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(),
            JavaRowFormat.CUSTOM);
    final JdbcConvention jdbcConvention =
        (JdbcConvention) child.getConvention();
    String sql = generateSql(jdbcConvention.dialect);
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("[" + sql + "]");
    }
    final Expression sqlLiteral =
        list.append("sql", Expressions.constant(sql));
    final List<Primitive> primitives = new ArrayList<>();
    for (int i = 0; i < getRowType().getFieldCount(); i++) {
      final Primitive primitive = Primitive.ofBoxOr(physType.fieldClass(i));
      primitives.add(primitive != null ? primitive : Primitive.OTHER);
    }
    final Expression primitivesLiteral =
        list.append("primitives",
            Expressions.constant(
                primitives.toArray(new Primitive[0])));
    final Expression enumerable =
        list.append(
            "enumerable",
            Expressions.call(
                BuiltInMethod.RESULT_SET_ENUMERABLE_OF.method,
                Expressions.call(
                    Expressions.convert_(
                        jdbcConvention.expression,
                        JdbcSchema.class),
                    BuiltInMethod.JDBC_SCHEMA_DATA_SOURCE.method),
                sqlLiteral,
                primitivesLiteral));
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  private String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, getInput());
    return result.asStatement().toSqlString(dialect).getSql();
  }
}

// End JdbcToSparkConverter.java
