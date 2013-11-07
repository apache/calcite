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
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRelImpl;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlDialect;

import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcToEnumerableConverter
    extends ConverterRelImpl
    implements EnumerableRel
{
  protected JdbcToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.instance, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new JdbcToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generate:
    //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
    final BlockBuilder list = new BlockBuilder();
    final JdbcRel child = (JdbcRel) getChild();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));
    final JdbcConvention jdbcConvention =
        (JdbcConvention) child.getConvention();
    String sql = generateSql(jdbcConvention.jdbcSchema.dialect);
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println("[" + sql + "]");
    }
    Hook.QUERY_PLAN.run(sql);
    final Expression sql_ =
        list.append("sql", Expressions.constant(sql));
    final int fieldCount = getRowType().getFieldCount();
    BlockBuilder builder = new BlockBuilder();
    final ParameterExpression resultSet_ =
        Expressions.parameter(Modifier.FINAL, ResultSet.class,
            builder.newName("resultSet"));
    final Expression calendar_;
    switch (CalendarPolicy.current()) {
    case NONE:
      calendar_ = null;
      break;
    case NULL:
    calendar_ =
        Expressions.convert_(Expressions.constant(null), Calendar.class);
      break;
    default:
      calendar_ =
          Expressions.call(
              Calendar.class,
              "getInstance",
              Expressions.convert_(
                  Expressions.call(
                      implementor.getRootExpression(),
                      "get",
                      Expressions.constant("timeZone")),
                  TimeZone.class));
    }
    if (fieldCount == 1) {
      final ParameterExpression value_ =
          Expressions.parameter(Object.class, builder.newName("value"));
      builder.add(Expressions.declare(0, value_, null));
      generateGet(physType, builder, resultSet_, 0, value_, calendar_);
      builder.add(Expressions.return_(null, value_));
    } else {
      final Expression values_ =
          builder.append("values",
              Expressions.newArrayBounds(Object.class, 1,
                  Expressions.constant(fieldCount)));
      for (int i = 0; i < fieldCount; i++) {
        generateGet(physType, builder, resultSet_, i,
            Expressions.arrayIndex(values_, Expressions.constant(i)),
            calendar_);
      }
      builder.add(
          Expressions.return_(null, values_));
    }
    final ParameterExpression e_ =
        Expressions.parameter(SQLException.class, builder.newName("e"));
    final Expression rowBuilderFactory_ =
        list.append("rowBuilderFactory",
            Expressions.lambda(
                Expressions.block(
                    Expressions.return_(null,
                        Expressions.lambda(
                            Expressions.block(
                                Expressions.tryCatch(
                                    builder.toBlock(),
                                    Expressions.catch_(
                                        e_,
                                        Expressions.throw_(
                                            Expressions.new_(
                                                RuntimeException.class,
                                                e_)))))))),
                resultSet_));
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
                sql_,
                rowBuilderFactory_));
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  private void generateGet(PhysType physType, BlockBuilder builder,
      ParameterExpression resultSet_, int i, Expression target,
      Expression calendar_) {
    final Primitive primitive = Primitive.ofBoxOr(physType.fieldClass(i));
    final Expression source;
    final RelDataType fieldType =
        physType.getRowType().getFieldList().get(i).getType();
    final List<Expression> dateTimeArgs = new ArrayList<Expression>();
    dateTimeArgs.add(Expressions.constant(i + 1));
    if (calendar_ != null) {
      dateTimeArgs.add(calendar_);
    }
    switch (fieldType.getSqlTypeName()) {
    case DATE:
      source = Expressions.call(
          BuiltinMethod.DATE_TO_INT.method,
          Expressions.call(resultSet_, "getDate", dateTimeArgs));
      break;
    case TIME:
      source = Expressions.call(
          BuiltinMethod.TIME_TO_INT.method,
          Expressions.call(resultSet_, "getTime", dateTimeArgs));
      break;
    case TIMESTAMP:
      source = Expressions.call(
          BuiltinMethod.TIMESTAMP_TO_LONG.method,
          Expressions.call(resultSet_, "getTimestamp", dateTimeArgs));
      break;
    default:
      source = Expressions.call(
          resultSet_, jdbcGetMethod(primitive), Expressions.constant(i + 1));
    }
    builder.add(
        Expressions.statement(
            Expressions.assign(
                target, source)));
    // TODO: add 'if ...' if nullable
  }

  /** E,g, {@code jdbcGetMethod(int)} returns "getInt". */
  private String jdbcGetMethod(Primitive primitive) {
    return primitive == null
        ? "getObject"
        : "get" + SqlFunctions.initcap(primitive.primitiveName);
  }

  private String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, getChild());
    return result.asQuery().toSqlString(dialect).getSql();
  }

  /** Whether this JDBC driver needs you to pass a Calendar object to methods
   * such as {@link ResultSet#getTimestamp(int, java.util.Calendar)}. */
  private enum CalendarPolicy {
    NONE,
    NULL,
    LOCAL;

    static CalendarPolicy current() {
      // NULL works for hsqldb-2.3; nothing worked for hsqldb-1.8.
      return NULL;
    }
  }
}

// End JdbcToEnumerableConverter.java
