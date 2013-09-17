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
package net.hydromatic.optiq.impl.clone;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.*;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends MapSchema {
  // TODO: implement 'driver' property
  // TODO: implement 'source' property
  // TODO: test Factory

  private final Schema sourceSchema;

  /**
   * Creates a CloneSchema.
   *
   * @param parentSchema Parent schema
   * @param name Name of schema
   * @param expression Expression for schema
   * @param sourceSchema JDBC data source
   */
  public CloneSchema(
      Schema parentSchema,
      String name,
      Expression expression,
      Schema sourceSchema) {
    super(parentSchema, name, expression);
    this.sourceSchema = sourceSchema;
  }

  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    // TODO: check elementType matches table.elementType
    assert elementType != null;

    Table<E> table = super.getTable(name, elementType);
    if (table != null) {
      return table;
    }
    // TODO: make thread safe!
    Table<E> sourceTable = sourceSchema.getTable(name, elementType);
    if (sourceTable != null) {
      //noinspection unchecked
      return createCloneTable(sourceTable, name);
    }
    return null;
  }

  private <T> Table<T> createCloneTable(Table<T> sourceTable, String name) {
    final TableInSchema tableInSchema =
        createCloneTable(this, name, sourceTable.getRowType(), sourceTable);
    addTable(tableInSchema);
    return tableInSchema.getTable(null);
  }

  public static <T> TableInSchema createCloneTable(MutableSchema schema,
      String name, RelDataType rowType, Enumerable<T> source) {
    final ColumnLoader loader =
        new ColumnLoader<T>(schema.getTypeFactory(), source, rowType);
    final Type elementType = source instanceof Queryable
        ? ((Queryable) source).getElementType()
        : Object.class;
    ArrayTable<T> table = new ArrayTable<T>(
        schema, elementType,
        rowType,
        Expressions.call(
            schema.getExpression(),
            BuiltinMethod.DATA_CONTEXT_GET_TABLE.method,
            Expressions.constant(name),
            Expressions.constant(Types.toClass(elementType))),
        loader.representationValues,
        loader.size(),
        loader.sortField);
    return new TableInSchemaImpl(schema, name, TableType.TABLE, table);
  }

  /**
   * Creates a CloneSchema within another schema.
   *
   * @param parentSchema Parent schema
   * @param name Name of new schema
   * @param sourceSchema Source schema
   * @return New CloneSchema
   */
  public static CloneSchema create(
      MutableSchema parentSchema,
      String name,
      Schema sourceSchema) {
    CloneSchema schema =
        new CloneSchema(
            parentSchema,
            name,
            parentSchema.getSubSchemaExpression(name, Object.class),
            sourceSchema);
    parentSchema.addSchema(name, schema);
    return schema;
  }

  /** Schema factory that creates a
   * {@link net.hydromatic.optiq.impl.clone.CloneSchema}.
   * This allows you to create a clone schema inside a model.json file.
   *
   * <pre>{@code
   * {
   *   version: '1.0',
   *   defaultSchema: 'FOODMART_CLONE',
   *   schemas: [
   *     {
   *       name: 'FOODMART_CLONE',
   *       type: 'custom',
   *       factory: 'net.hydromatic.optiq.impl.clone.CloneSchema.Factory',
   *       operand: {
   *         driver: 'com.mysql.jdbc.Driver',
   *         url: 'jdbc:mysql://localhost/foodmart',
   *         user: 'foodmart',
   *         password: 'foodmart'
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   */
  public static class Factory implements SchemaFactory {
    public Schema create(
        MutableSchema parentSchema,
        String name,
        Map<String, Object> operand) {
      JdbcSchema jdbcSchema =
          JdbcSchema.create(parentSchema, name + "$source", operand);
      return CloneSchema.create(parentSchema, name, jdbcSchema);
    }
  }
}

// End CloneSchema.java
