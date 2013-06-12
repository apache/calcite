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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.runtime.ResultSetEnumerable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.pretty.SqlPrettyWriter;
import org.eigenbase.sql.util.SqlString;

import java.lang.reflect.Type;
import java.sql.ResultSet;
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
class JdbcTable extends AbstractQueryable<Object[]>
    implements TranslatableTable<Object[]>
{
  private final JdbcSchema schema;
  public final String tableName;
  private final RelDataType rowType;

  public JdbcTable(
      RelDataType rowType,
      JdbcSchema schema,
      String tableName) {
    this.rowType = rowType;
    this.schema = schema;
    this.tableName = tableName;
    assert rowType != null;
    assert schema != null;
    assert tableName != null;
  }

  public String toString() {
    return "JdbcTable {" + tableName + "}";
  }

  public QueryProvider getProvider() {
    return schema.queryProvider;
  }

  public DataContext getDataContext() {
    return schema;
  }

  public Type getElementType() {
    return Object[].class;
  }

  public Expression getExpression() {
    return Expressions.call(
        schema.getExpression(),
        BuiltinMethod.DATA_CONTEXT_GET_TABLE.method,
        Expressions.<Expression>list()
            .append(Expressions.constant(tableName))
            .append(Expressions.constant(getElementType())));
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public Iterator<Object[]> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<Object[]> enumerator() {
    final SqlString sql = generateSql();
    Function1<ResultSet, Function0<Object[]>> rowBuilderFactory =
        JdbcUtils.ObjectArrayRowBuilder.factory(
            JdbcUtils.getPrimitives(
                schema.typeFactory, rowType));
    return ResultSetEnumerable.of(
        schema.getDataSource(),
        sql.getSql(),
        rowBuilderFactory).enumerator();
  }

  SqlString generateSql() {
    SqlSelect node =
        SqlStdOperatorTable.selectOperator.createCall(
            SqlNodeList.Empty,
            new SqlNodeList(
                Collections.singletonList(
                    new SqlIdentifier("*", SqlParserPos.ZERO)),
                SqlParserPos.ZERO),
            tableName(), null, null, null, null, null, SqlParserPos.ZERO);
    final SqlPrettyWriter writer = new SqlPrettyWriter(SqlDialect.DUMMY);
    node.unparse(writer, 0, 0);
    return writer.toSqlString();
  }

  SqlIdentifier tableName() {
    final List<String> strings = new ArrayList<String>();
    if (schema.catalog != null) {
      strings.add(schema.catalog);
    }
    if (schema.schema != null) {
      strings.add(schema.schema);
    }
    strings.add(tableName);
    return new SqlIdentifier(
        strings.toArray(new String[strings.size()]),
        SqlParserPos.ZERO);
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new JdbcTableScan(
        context.getCluster(), relOptTable, this, schema.convention);
  }
}

// End JdbcTable.java
