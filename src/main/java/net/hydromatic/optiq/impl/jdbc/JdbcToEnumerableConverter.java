/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRelImpl;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcToEnumerableConverter
    extends ConverterRelImpl
    implements EnumerableRel
{
  private final PhysType physType;

  protected JdbcToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.instance, traits, input);
    this.physType =
        PhysTypeImpl.of(
            (JavaTypeFactory) cluster.getTypeFactory(),
            getRowType(),
            (EnumerableConvention) getConvention());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new JdbcToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  public PhysType getPhysType() {
    return physType;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);
  }

  public BlockExpression implement(EnumerableRelImplementor implementor) {
    // Generate:
    //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
    final BlockBuilder list = new BlockBuilder();
    final JdbcRel child = (JdbcRel) getChild();
    final JdbcConvention jdbcConvention =
        (JdbcConvention) child.getConvention();
    String sql = generateSql(jdbcConvention.jdbcSchema.dialect);
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println("[" + sql + "]");
    }
    final Expression sqlLiteral =
        list.append("sql", Expressions.constant(sql));
    final List<Primitive> primitives = new ArrayList<Primitive>();
    for (int i = 0; i < getRowType().getFieldCount(); i++) {
      final Primitive primitive = Primitive.ofBoxOr(physType.fieldClass(i));
      primitives.add(primitive != null ? primitive : Primitive.OTHER);
    }
    final Expression primitivesLiteral =
        list.append("primitives",
            Expressions.constant(
                primitives.toArray(new Primitive[primitives.size()])));
    final Expression enumerable =
        list.append(
            "enumerable",
            Expressions.call(
                BuiltinMethod.RESULT_SET_ENUMERABLE_OF.method,
                Expressions.call(
                    Expressions.convert_(
                        jdbcConvention.jdbcSchema.getExpression(),
                        JdbcSchema.class),
                    BuiltinMethod.JDBC_SCHEMA_DATA_SOURCE.method),
                sqlLiteral,
                primitivesLiteral));
    list.add(
        Expressions.return_(null, enumerable));
    return list.toBlock();
  }

  private String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, getChild());
    return result.asQuery().toSqlString(dialect).getSql();
  }
}

// End JdbcToEnumerableConverter.java
