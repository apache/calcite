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
package net.hydromatic.optiq.impl.clone;

import net.hydromatic.avatica.ColumnMetaData;

import net.hydromatic.linq4j.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.impl.java.*;
import net.hydromatic.optiq.impl.jdbc.JdbcSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.reltype.RelProtoDataType;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static net.hydromatic.optiq.impl.MaterializedViewTable.MATERIALIZATION_CONNECTION;

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
public class CloneSchema extends AbstractSchema {
  // TODO: implement 'driver' property
  // TODO: implement 'source' property
  // TODO: test Factory

  private final SchemaPlus sourceSchema;

  /**
   * Creates a CloneSchema.
   *
   * @param sourceSchema JDBC data source
   */
  public CloneSchema(SchemaPlus sourceSchema) {
    super();
    this.sourceSchema = sourceSchema;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    final Map<String, Table> map = new LinkedHashMap<String, Table>();
    for (String name : sourceSchema.getTableNames()) {
      final Table table = sourceSchema.getTable(name);
      if (table instanceof QueryableTable) {
        final QueryableTable sourceTable = (QueryableTable) table;
        map.put(name,
            createCloneTable(MATERIALIZATION_CONNECTION, sourceTable, name));
      }
    }
    return map;
  }

  private Table createCloneTable(QueryProvider queryProvider,
      QueryableTable sourceTable, String name) {
    final Queryable<Object> queryable =
        sourceTable.asQueryable(queryProvider, sourceSchema, name);
    final JavaTypeFactory typeFactory =
        ((OptiqConnection) queryProvider).getTypeFactory();
    return createCloneTable(typeFactory, Schemas.proto(sourceTable), null,
        queryable);
  }

  public static <T> Table createCloneTable(final JavaTypeFactory typeFactory,
      final RelProtoDataType protoRowType,
      final List<ColumnMetaData.Rep> repList,
      final Enumerable<T> source) {
    final Type elementType = source instanceof QueryableTable
        ? ((QueryableTable) source).getElementType()
        : Object[].class;
    return new ArrayTable(
        elementType,
        protoRowType,
        Suppliers.memoize(
            new Supplier<ArrayTable.Content>() {
              public ArrayTable.Content get() {
                final ColumnLoader loader =
                    new ColumnLoader<T>(typeFactory, source, protoRowType,
                        repList);
                return new ArrayTable.Content(loader.representationValues,
                    loader.size(), loader.sortField);
              }
            }));
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
   *       factory: 'net.hydromatic.optiq.impl.clone.CloneSchema$Factory',
   *       operand: {
   *         jdbcDriver: 'com.mysql.jdbc.Driver',
   *         jdbcUrl: 'jdbc:mysql://localhost/foodmart',
   *         jdbcUser: 'foodmart',
   *         jdbcPassword: 'foodmart'
   *       }
   *     }
   *   ]
   * }
   * }</pre>
   */
  public static class Factory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        Map<String, Object> operand) {
      SchemaPlus schema =
          parentSchema.add(name,
              JdbcSchema.create(parentSchema, name + "$source", operand));
      return new CloneSchema(schema);
    }
  }
}

// End CloneSchema.java
