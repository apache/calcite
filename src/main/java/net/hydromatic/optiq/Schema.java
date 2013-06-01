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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A namespace for tables and table functions.
 *
 * <p>A schema can also contain sub-schemas, to any level of nesting. Most
 * providers have a limited number of levels; for example, most JDBC databases
 * have either one level ("schemas") or two levels ("database" and
 * "catalog").</p>
 *
 * <p>There may be multiple overloaded table functions with the same name but
 * different numbers or types of parameters.
 * For this reason, {@link #getTableFunctions} returns a list of all
 * members with the same name. Optiq will call
 * {@link Schemas#resolve(String, java.util.List, java.util.List)} to choose the
 * appropriate one.</p>
 *
 * <p>The most common and important type of member is the one with no
 * arguments and a result type that is a collection of records. This is called a
 * <dfn>relation</dfn>. It is equivalent to a table in a relational
 * database.</p>
 *
 * <p>For example, the query</p>
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * <p>is valid if "sales" is a registered
 * schema and "emps" is a member with zero parameters and a result type
 * of <code>Collection(Record(int: "empno", String: "name"))</code>.</p>
 *
 * <p>A schema may be nested within another schema; see
 * {@link Schema#getSubSchema(String)}.</p>
 */
public interface Schema extends DataContext {

  /**
   * Returns a list of table functions in this schema with the given name, or
   * an empty list if there is no such table function.
   *
   * @param name Name of table function
   * @return List of table functions with given name, or empty list
   */
  List<TableFunction> getTableFunctions(String name);

  /**
   * Returns a table with the given name, or null.
   *
   * @param name Table name
   * @param elementType Element type
   * @return Table, or null
   */
  <E> Table<E> getTable(String name, Class<E> elementType);

  Expression getExpression();

  QueryProvider getQueryProvider();

  Map<String, List<TableFunction>> getTableFunctions();

  Collection<String> getSubSchemaNames();

  Collection<TableInSchema> getTables();

  abstract class TableInSchema {
    public final Schema schema;
    public final String name;
    public final TableType tableType;

    public TableInSchema(
        Schema schema, String name, TableType tableType) {
      this.schema = schema;
      this.name = name;
      this.tableType = tableType;
    }

    public abstract <E> Table<E> getTable(Class<E> elementType);
  }

  enum TableType {
    TABLE, VIEW, SYSTEM_TABLE, LOCAL_TEMPORARY,
  }
}

// End Schema.java
