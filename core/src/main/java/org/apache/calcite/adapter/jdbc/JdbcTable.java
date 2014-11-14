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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.runtime.ResultSetEnumerable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.pretty.SqlPrettyWriter;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import java.sql.SQLException;
import java.util.*;

/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * <p>The idea is not to read the whole table, however. The idea is to use
 * this as a building block for a query, by applying Queryable operators
 * such as {@link net.hydromatic.linq4j.Queryable#where(net.hydromatic.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be
 * executed efficiently on the JDBC server.</p>
 */
class JdbcTable extends AbstractQueryableTable implements TranslatableTable {
  private RelProtoDataType protoRowType;
  private final JdbcSchema jdbcSchema;
  private final String jdbcCatalogName;
  private final String jdbcSchemaName;
  private final String jdbcTableName;
  private final Schema.TableType jdbcTableType;

  public JdbcTable(JdbcSchema jdbcSchema, String jdbcCatalogName,
      String jdbcSchemaName, String tableName, Schema.TableType jdbcTableType) {
    super(Object[].class);
    this.jdbcSchema = jdbcSchema;
    this.jdbcCatalogName = jdbcCatalogName;
    this.jdbcSchemaName = jdbcSchemaName;
    this.jdbcTableName = tableName;
    this.jdbcTableType = jdbcTableType;
  }

  public String toString() {
    return "JdbcTable {" + jdbcTableName + "}";
  }

  @Override public Schema.TableType getJdbcTableType() {
    return jdbcTableType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType == null) {
      try {
        protoRowType =
            jdbcSchema.getRelDataType(
                jdbcCatalogName,
                jdbcSchemaName,
                jdbcTableName);
      } catch (SQLException e) {
        throw new RuntimeException(
            "Exception while reading definition of table '" + jdbcTableName
            + "'", e);
      }
    }
    return protoRowType.apply(typeFactory);
  }

  private List<Pair<Primitive, Integer>> fieldClasses(
      final JavaTypeFactory typeFactory) {
    final RelDataType rowType = protoRowType.apply(typeFactory);
    return Functions.adapt(
        rowType.getFieldList(),
        new Function1<RelDataTypeField, Pair<Primitive, Integer>>() {
          public Pair<Primitive, Integer> apply(RelDataTypeField field) {
            RelDataType type = field.getType();
            Class clazz = (Class) typeFactory.getJavaClass(type);
            return Pair.of(Util.first(Primitive.of(clazz), Primitive.OTHER),
                type.getSqlTypeName().getJdbcOrdinal());
          }
        });
  }

  SqlString generateSql() {
    final SqlNodeList selectList =
        new SqlNodeList(
            Collections.singletonList(
                new SqlIdentifier("*", SqlParserPos.ZERO)),
            SqlParserPos.ZERO);
    SqlSelect node =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList,
            tableName(), null, null, null, null, null, null, null);
    final SqlPrettyWriter writer = new SqlPrettyWriter(jdbcSchema.dialect);
    node.unparse(writer, 0, 0);
    return writer.toSqlString();
  }

  SqlIdentifier tableName() {
    final List<String> strings = new ArrayList<String>();
    if (jdbcSchema.catalog != null) {
      strings.add(jdbcSchema.catalog);
    }
    if (jdbcSchema.schema != null) {
      strings.add(jdbcSchema.schema);
    }
    strings.add(jdbcTableName);
    return new SqlIdentifier(strings, SqlParserPos.ZERO);
  }

  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new JdbcTableScan(context.getCluster(), relOptTable, this,
        jdbcSchema.convention);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new JdbcTableQueryable<T>(queryProvider, schema, tableName);
  }

  /** Enumerable that returns the contents of a {@link JdbcTable} by connecting
   * to the JDBC data source. */
  private class JdbcTableQueryable<T> extends AbstractTableQueryable<T> {
    public JdbcTableQueryable(QueryProvider queryProvider, SchemaPlus schema,
        String tableName) {
      super(queryProvider, schema, JdbcTable.this, tableName);
    }

    @Override public String toString() {
      return "JdbcTableQueryable {table: " + tableName + "}";
    }

    public Enumerator<T> enumerator() {
      final JavaTypeFactory typeFactory =
          ((OptiqConnection) queryProvider).getTypeFactory();
      final SqlString sql = generateSql();
      //noinspection unchecked
      final Enumerable<T> enumerable = (Enumerable<T>) ResultSetEnumerable.of(
          jdbcSchema.getDataSource(),
          sql.getSql(),
          JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
      return enumerable.enumerator();
    }
  }
}

// End JdbcTable.java
