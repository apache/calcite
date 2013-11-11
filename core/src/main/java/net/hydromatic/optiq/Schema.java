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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import com.google.common.collect.Multimap;

import java.util.*;

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
 * {@link Schemas#resolve(String, java.util.Collection, java.util.List)}
 * to choose the appropriate one.</p>
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
public interface Schema {
  /**
   * Returns the parent schema, or null if this schema has no parent.
   */
  Schema getParentSchema();

  /**
   * Returns the name of this schema.
   *
   * <p>The name must not be null, and must be unique within its parent.
   * The root schema is typically named "".
   */
  String getName();

  /**
   * Returns a list of table functions in this schema with the given name, or
   * an empty list if there is no such table function.
   *
   * @param name Name of table function
   * @return List of table functions with given name, or empty list
   */
  Collection<TableFunctionInSchema> getTableFunctions(String name);

  /**
   * Returns a table with a given name and element type, or null if not found.
   *
   * @param name Table name
   * @param elementType Element type
   * @return Table, or null
   */
  <E> Table<E> getTable(String name, Class<E> elementType);

  Expression getExpression();

  QueryProvider getQueryProvider();

  Multimap<String, TableFunctionInSchema> getTableFunctions();

  /**
   * Returns a sub-schema with a given name, or null.
   */
  Schema getSubSchema(String name);

  Collection<String> getSubSchemaNames();

  Map<String, TableInSchema> getTables();

  JavaTypeFactory getTypeFactory();

  abstract class ObjectInSchema {
    public final Schema schema;
    public final String name;

    public ObjectInSchema(Schema schema, String name) {
      Linq4j.requireNonNull(schema);
      Linq4j.requireNonNull(name);
      this.schema = schema;
      this.name = name;
    }

    /** Returns this object's path. For example ["hr", "emps"]. */
    public final List<String> path() {
      return Schemas.path(schema, name);
    }
  }

  abstract class TableInSchema extends ObjectInSchema {
    public final TableType tableType;

    public TableInSchema(
        Schema schema, String name, TableType tableType) {
      super(schema, name);
      this.tableType = tableType;
    }

    public abstract <E> Table<E> getTable(Class<E> elementType);
  }

  abstract class TableFunctionInSchema extends ObjectInSchema {
    public TableFunctionInSchema(Schema schema, String name) {
      super(schema, name);
    }

    public abstract TableFunction getTableFunction();

    /** Whether this represents a materialized view. (At a given point in time,
     * it may or may not be materialized as a table.) */
    public abstract boolean isMaterialization();
  }

  enum TableType {
    /** A regular table. */
    TABLE,
    /** A relation whose contents are calculated by evaluating a SQL
     * expression. */
    VIEW,
    /** A table maintained by the system. Data dictionary tables, such as the
     * "TABLES" and "COLUMNS" table in the "metamodel" schema, examples of
     * system tables. */
    SYSTEM_TABLE,
    /** A table that is only visible to one connection. */
    LOCAL_TEMPORARY,
  }
}

// End Schema.java
