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
package org.apache.calcite.adapter.clone;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.schema.impl.MaterializedViewTable.MATERIALIZATION_CONNECTION;

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

  @Override protected Map<String, Table> getTableMap() {
    final Map<String, Table> map = new LinkedHashMap<>();
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
        ((CalciteConnection) queryProvider).getTypeFactory();
    return createCloneTable(typeFactory, Schemas.proto(sourceTable),
        ImmutableList.of(), null, queryable);
  }

  @Deprecated // to be removed before 2.0
  public static <T> Table createCloneTable(final JavaTypeFactory typeFactory,
      final RelProtoDataType protoRowType,
      final List<ColumnMetaData.Rep> repList,
      final Enumerable<T> source) {
    return createCloneTable(typeFactory, protoRowType, ImmutableList.of(),
        repList, source);
  }

  public static <T> Table createCloneTable(final JavaTypeFactory typeFactory,
      final RelProtoDataType protoRowType, final List<RelCollation> collations,
      final List<ColumnMetaData.Rep> repList, final Enumerable<T> source) {
    final Type elementType;
    if (source instanceof QueryableTable) {
      elementType = ((QueryableTable) source).getElementType();
    } else if (protoRowType.apply(typeFactory).getFieldCount() == 1) {
      if (repList != null) {
        elementType = repList.get(0).clazz;
      } else {
        elementType = Object.class;
      }
    } else {
      elementType = Object[].class;
    }
    return new ArrayTable(
        elementType,
        protoRowType,
        Suppliers.memoize(() -> {
          final ColumnLoader loader =
              new ColumnLoader<>(typeFactory, source, protoRowType,
                  repList);
          final List<RelCollation> collation2 =
              collations.isEmpty()
                  && loader.sortField >= 0
                  ? RelCollations.createSingleton(loader.sortField)
                  : collations;
          return new ArrayTable.Content(loader.representationValues,
              loader.size(), collation2);
        }));
  }

  /** Schema factory that creates a
   * {@link org.apache.calcite.adapter.clone.CloneSchema}.
   * This allows you to create a clone schema inside a model.json file.
   *
   * <blockquote><pre>
   * {
   *   version: '1.0',
   *   defaultSchema: 'FOODMART_CLONE',
   *   schemas: [
   *     {
   *       name: 'FOODMART_CLONE',
   *       type: 'custom',
   *       factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',
   *       operand: {
   *         jdbcDriver: 'com.mysql.jdbc.Driver',
   *         jdbcUrl: 'jdbc:mysql://localhost/foodmart',
   *         jdbcUser: 'foodmart',
   *         jdbcPassword: 'foodmart'
   *       }
   *     }
   *   ]
   * }</pre></blockquote>
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
