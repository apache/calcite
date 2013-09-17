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
package net.hydromatic.optiq.impl.java;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.Table;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

/**
 * Implementation of {@link Schema} backed by a {@link HashMap}.
 */
public class MapSchema implements MutableSchema {
  private final Schema parentSchema;
  private final QueryProvider queryProvider;
  protected final JavaTypeFactory typeFactory;
  private final String name;
  private final Expression expression;

  protected final Map<String, TableInSchema> tableMap =
      new HashMap<String, TableInSchema>();

  protected final Multimap<String, TableFunctionInSchema> membersMap =
      LinkedListMultimap.create();

  protected final Map<String, Schema> subSchemaMap =
      new HashMap<String, Schema>();

  /**
   * Creates a MapSchema.
   *
   * @param parentSchema Parent schema (may be null)
   * @param queryProvider Query provider
   * @param typeFactory Type factory
   * @param name Name of schema
   * @param expression Expression for schema
   */
  public MapSchema(
      Schema parentSchema,
      QueryProvider queryProvider,
      JavaTypeFactory typeFactory,
      String name,
      Expression expression) {
    this.parentSchema = parentSchema;
    this.queryProvider = queryProvider;
    this.typeFactory = typeFactory;
    this.name = name;
    this.expression = expression;

    assert expression != null;
    assert typeFactory != null;
    assert queryProvider != null;
    assert name != null;
  }

  /**
   * Creates a MapSchema that is a sub-schema.
   *
   * @param parentSchema Parent schema
   * @param name Name of schema
   * @param expression Expression for schema
   *
   * @throws NullPointerException if parentSchema is null
   */
  public MapSchema(
      Schema parentSchema,
      String name,
      Expression expression) {
    this(
        parentSchema,
        parentSchema.getQueryProvider(),
        parentSchema.getTypeFactory(),
        name,
        expression);
  }

  /**
   * Creates a MapSchema within another schema.
   *
   * @param parentSchema Parent schema
   * @param name Name of new schema
   * @return New MapSchema
   */
  public static MapSchema create(
      MutableSchema parentSchema,
      String name) {
    MapSchema schema =
        new MapSchema(
            parentSchema,
            name,
            parentSchema.getSubSchemaExpression(name, Object.class));
    parentSchema.addSchema(name, schema);
    return schema;
  }

  /** Called by Optiq after creation, before loading tables explicitly defined
   * in a JSON model. */
  public void initialize() {
    for (TableInSchema tableInSchema : initialTables()) {
      tableMap.put(tableInSchema.name, tableInSchema);
    }
  }

  public Schema getParentSchema() {
    return parentSchema;
  }

  public String getName() {
    return name;
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public Expression getExpression() {
    return expression;
  }

  public QueryProvider getQueryProvider() {
    return queryProvider;
  }

  public Map<String, TableInSchema> getTables() {
    return tableMap;
  }

  public <E> Table<E> getTable(String name, Class<E> elementType) {
    // TODO: check elementType matches table.elementType
    assert elementType != null;

    // First look for a table.
    TableInSchema table = tableMap.get(name);
    if (table != null) {
      return table.getTable(elementType);
    }
    // Then look for a table-function with no arguments.
    Collection<TableFunctionInSchema> tableFunctions = membersMap.get(name);
    if (tableFunctions != null) {
      for (TableFunctionInSchema tableFunctionInSchema : tableFunctions) {
        TableFunction tableFunction = tableFunctionInSchema.getTableFunction();
        if (tableFunction.getParameters().isEmpty()) {
          //noinspection unchecked
          return tableFunction.apply(Collections.emptyList());
        }
      }
    }
    return null;
  }

  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return membersMap;
  }

  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return membersMap.get(name);
  }

  public Collection<String> getSubSchemaNames() {
    return subSchemaMap.keySet();
  }

  public Schema getSubSchema(String name) {
    return subSchemaMap.get(name);
  }

  public void addTableFunction(TableFunctionInSchema tableFunctionInSchema) {
    membersMap.put(tableFunctionInSchema.name, tableFunctionInSchema);
  }

  public void addTable(TableInSchema table) {
    tableMap.put(table.name, table);
  }

  public void addSchema(String name, Schema schema) {
    subSchemaMap.put(name, schema);
  }

  public Expression getSubSchemaExpression(String name, Class type) {
    // (Type) schemaExpression.getSubSchema("name")
    Expression call =
        Expressions.call(
            getExpression(),
            BuiltinMethod.GET_SUB_SCHEMA.method,
            Expressions.constant(name));
    //noinspection unchecked
    if (type != null && !type.isAssignableFrom(Schema.class)) {
      return Expressions.convert_(call, type);
    }
    return call;
  }

  /** Returns the initial set of tables.
   *
   * <p>The default implementation returns an empty list. Derived classes
   * may override this method to create tables based on their schema type. For
   * example, a CSV provider might scan for all ".csv" files in a particular
   * directory and return a table for each.</p>
   */
  protected Collection<TableInSchema> initialTables() {
    return Collections.emptyList();
  }
}

// End MapSchema.java
