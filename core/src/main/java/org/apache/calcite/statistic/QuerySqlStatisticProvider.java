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
package org.apache.calcite.statistic;

import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * Implementation of {@link SqlStatisticProvider} that generates and executes
 * SQL queries.
 */
public class QuerySqlStatisticProvider implements SqlStatisticProvider {
  /** Instance that uses SQL to compute statistics,
   * does not log SQL statements,
   * and caches up to 1,024 results for up to 30 minutes.
   * (That period should be sufficient for the
   * duration of Calcite's tests, and many other purposes.) */
  public static final SqlStatisticProvider SILENT_CACHING_INSTANCE =
      new CachingSqlStatisticProvider(
          new QuerySqlStatisticProvider(sql -> { }),
          CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES)
              .maximumSize(1_024).build());

  /** As {@link #SILENT_CACHING_INSTANCE} but prints SQL statements to
   * {@link System#out}. */
  public static final SqlStatisticProvider VERBOSE_CACHING_INSTANCE =
      new CachingSqlStatisticProvider(
          new QuerySqlStatisticProvider(sql -> System.out.println(sql + ":")),
          CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES)
              .maximumSize(1_024).build());

  private final Consumer<String> sqlConsumer;

  /** Creates a QuerySqlStatisticProvider.
   *
   * @param sqlConsumer Called when each SQL statement is generated
   */
  public QuerySqlStatisticProvider(Consumer<String> sqlConsumer) {
    this.sqlConsumer = Objects.requireNonNull(sqlConsumer);
  }

  public double tableCardinality(RelOptTable table) {
    final JdbcTable jdbcTable = table.unwrap(JdbcTable.class);
    return withBuilder(jdbcTable.jdbcSchema,
        (cluster, relOptSchema, jdbcSchema, relBuilder) -> {
          // Generate:
          //   SELECT COUNT(*) FROM `EMP`
          relBuilder.push(table.toRel(ViewExpanders.simpleContext(cluster)))
              .aggregate(relBuilder.groupKey(), relBuilder.count());

          final String sql = toSql(relBuilder.build(), jdbcSchema.dialect);
          final DataSource dataSource = jdbcSchema.getDataSource();
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement();
               ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
              throw new AssertionError("expected exactly 1 row: " + sql);
            }
            final double cardinality = resultSet.getDouble(1);
            if (resultSet.next()) {
              throw new AssertionError("expected exactly 1 row: " + sql);
            }
            return cardinality;
          } catch (SQLException e) {
            throw handle(e, sql);
          }
        });
  }

  public boolean isForeignKey(RelOptTable fromTable, List<Integer> fromColumns,
      RelOptTable toTable, List<Integer> toColumns) {
    final JdbcTable jdbcTable = fromTable.unwrap(JdbcTable.class);
    return withBuilder(jdbcTable.jdbcSchema,
        (cluster, relOptSchema, jdbcSchema, relBuilder) -> {
          // EMP(DEPTNO) is a foreign key to DEPT(DEPTNO) if the following
          // query returns 0:
          //
          //   SELECT COUNT(*) FROM (
          //     SELECT deptno FROM `EMP` WHERE deptno IS NOT NULL
          //     MINUS
          //     SELECT deptno FROM `DEPT`)
          final RelOptTable.ToRelContext toRelContext =
              ViewExpanders.simpleContext(cluster);
          relBuilder.push(fromTable.toRel(toRelContext))
              .filter(fromColumns.stream()
                  .map(column ->
                      relBuilder.call(SqlStdOperatorTable.IS_NOT_NULL,
                          relBuilder.field(column)))
                  .collect(Collectors.toList()))
              .project(relBuilder.fields(fromColumns))
              .push(toTable.toRel(toRelContext))
              .project(relBuilder.fields(toColumns))
              .minus(false, 2)
              .aggregate(relBuilder.groupKey(), relBuilder.count());

          final String sql = toSql(relBuilder.build(), jdbcSchema.dialect);
          final DataSource dataSource = jdbcTable.jdbcSchema.getDataSource();
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement();
               ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
              throw new AssertionError("expected exactly 1 row: " + sql);
            }
            final int count = resultSet.getInt(1);
            if (resultSet.next()) {
              throw new AssertionError("expected exactly 1 row: " + sql);
            }
            return count == 0;
          } catch (SQLException e) {
            throw handle(e, sql);
          }
        });
  }

  public boolean isKey(RelOptTable table, List<Integer> columns) {
    final JdbcTable jdbcTable = table.unwrap(JdbcTable.class);
    return withBuilder(jdbcTable.jdbcSchema,
        (cluster, relOptSchema, jdbcSchema, relBuilder) -> {
          // The collection of columns ['DEPTNO'] is a key for 'EMP' if the
          // following query returns no rows:
          //
          //   SELECT 1
          //   FROM `EMP`
          //   GROUP BY `DEPTNO`
          //   HAVING COUNT(*) > 1
          //
          final RelOptTable.ToRelContext toRelContext =
              ViewExpanders.simpleContext(cluster);
          relBuilder.push(table.toRel(toRelContext))
              .aggregate(relBuilder.groupKey(relBuilder.fields(columns)),
                  relBuilder.count())
              .filter(
                  relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
                      Util.last(relBuilder.fields()), relBuilder.literal(1)));
          final String sql = toSql(relBuilder.build(), jdbcSchema.dialect);

          final DataSource dataSource = jdbcSchema.getDataSource();
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement();
               ResultSet resultSet = statement.executeQuery(sql)) {
            return !resultSet.next();
          } catch (SQLException e) {
            throw handle(e, sql);
          }
        });
  }

  private RuntimeException handle(SQLException e, String sql) {
    return new RuntimeException("Error while executing SQL for statistics: "
        + sql, e);
  }

  protected String toSql(RelNode rel, SqlDialect dialect) {
    final RelToSqlConverter converter = new RelToSqlConverter(dialect);
    SqlImplementor.Result result = converter.visitChild(0, rel);
    final SqlNode sqlNode = result.asStatement();
    final String sql = sqlNode.toSqlString(dialect).getSql();
    sqlConsumer.accept(sql);
    return sql;
  }

  private <R> R withBuilder(JdbcSchema jdbcSchema, BuilderAction<R> action) {
    return Frameworks.withPlanner(
        (cluster, relOptSchema, rootSchema) -> {
          final RelBuilder relBuilder =
              JdbcRules.JDBC_BUILDER.create(cluster, relOptSchema);
          return action.apply(cluster, relOptSchema, jdbcSchema, relBuilder);
        });
  }

  /** Performs an action with a {@link RelBuilder}.
   *
   * @param <R> Result type */
  private interface BuilderAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        JdbcSchema jdbcSchema, RelBuilder relBuilder);
  }
}

// End QuerySqlStatisticProvider.java
