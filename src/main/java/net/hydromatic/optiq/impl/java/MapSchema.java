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

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Implementation of {@link Schema} backed by a {@link HashMap}.
 */
public class MapSchema implements MutableSchema {

  protected final Map<String, TableInSchema> tableMap =
      new HashMap<String, TableInSchema>();

  protected final Map<String, List<TableFunction>> membersMap =
      new HashMap<String, List<TableFunction>>();

  protected final Map<String, Schema> subSchemaMap =
      new HashMap<String, Schema>();

  private final QueryProvider queryProvider;
  protected final JavaTypeFactory typeFactory;
  private final Expression expression;

  /**
   * Creates a MapSchema.
   *
   * @param queryProvider Query provider
   * @param typeFactory Type factory
   * @param expression Expression for schema
   */
  public MapSchema(
      QueryProvider queryProvider,
      JavaTypeFactory typeFactory,
      Expression expression) {
    this.queryProvider = queryProvider;
    this.typeFactory = typeFactory;
    this.expression = expression;

    assert expression != null;
    assert typeFactory != null;
    assert queryProvider != null;
  }

  /**
   * Creates a MapSchema that is a sub-schema.
   *
   * @param parentSchema Parent schema
   * @param expression Expression for schema
   */
  public MapSchema(
      Schema parentSchema,
      Expression expression) {
    this(
        parentSchema.getQueryProvider(),
        parentSchema.getTypeFactory(),
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

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public Expression getExpression() {
    return expression;
  }

  public QueryProvider getQueryProvider() {
    return queryProvider;
  }

  public Collection<TableInSchema> getTables() {
    return tableMap.values();
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
    List<TableFunction> tableFunctions = membersMap.get(name);
    if (tableFunctions != null) {
      for (TableFunction tableFunction : tableFunctions) {
        if (tableFunction.getParameters().isEmpty()) {
          //noinspection unchecked
          return tableFunction.apply(Collections.emptyList());
        }
      }
    }
    return null;
  }

  public Map<String, List<TableFunction>> getTableFunctions() {
    return membersMap;
  }

  public List<TableFunction> getTableFunctions(String name) {
    List<TableFunction> members = membersMap.get(name);
    if (members != null) {
      return members;
    }
    return Collections.emptyList();
  }

  public Collection<String> getSubSchemaNames() {
    return subSchemaMap.keySet();
  }

  public Schema getSubSchema(String name) {
    return subSchemaMap.get(name);
  }

  public void addTableFunction(String name, TableFunction tableFunction) {
    putMulti(membersMap, name, tableFunction);
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
            Collections.<Expression>singletonList(
                Expressions.constant(name)));
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

  private Type deduceType(Object schemaObject) {
    // REVIEW: Can we remove the dependency on RelDataType and work in
    //   terms of Class?
    if (schemaObject instanceof Member) {
      RelDataType type = ((Member) schemaObject).getType();
      return typeFactory.getJavaClass(type);
    }
    if (schemaObject instanceof Schema) {
      return schemaObject.getClass();
    }
    return null;
  }

  protected static <K, V> void putMulti(
      Map<K, List<V>> map, K k, V v) {
    List<V> list = map.put(k, Collections.singletonList(v));
    if (list == null) {
      return;
    }
    if (list.size() == 1) {
      list = new ArrayList<V>(list);
    }
    list.add(v);
    map.put(k, list);
  }
}

// End MapSchema.java
