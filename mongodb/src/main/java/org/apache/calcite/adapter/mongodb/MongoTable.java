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
package org.apache.calcite.adapter.mongodb;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryableTable
    implements TranslatableTable {
  private final String collectionName;

  /** Creates a MongoTable. */
  MongoTable(String collectionName) {
    super(Object[].class);
    this.collectionName = collectionName;
  }

  public String toString() {
    return "MongoTable {" + collectionName + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true));
    return typeFactory.builder().add("_MAP", mapType).build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new MongoQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new MongoTableScan(cluster, cluster.traitSetOf(MongoRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Executes a "find" operation on the underlying collection.
   *
   * <p>For example,
   * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code></p>
   *
   * @param mongoDb MongoDB connection
   * @param filterJson Filter JSON string, or null
   * @param projectJson Project JSON string, or null
   * @param fields List of fields to project; or null to return map
   * @return Enumerator of results
   */
  private Enumerable<Object> find(MongoDatabase mongoDb, String filterJson,
      String projectJson, List<Map.Entry<String, Class>> fields) {
    final MongoCollection collection =
        mongoDb.getCollection(collectionName);
    final Bson filter =
        filterJson == null ? null : BsonDocument.parse(filterJson);
    final Bson project =
        projectJson == null ? null : BsonDocument.parse(projectJson);
    final Function1<Document, Object> getter = MongoEnumerator.getter(fields);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        @SuppressWarnings("unchecked") final FindIterable<Document> cursor =
            collection.find(filter).projection(project);
        return new MongoEnumerator(cursor.iterator(), getter);
      }
    };
  }

  /** Executes an "aggregate" operation on the underlying collection.
   *
   * <p>For example:
   * <code>zipsTable.aggregate(
   * "{$filter: {state: 'OR'}",
   * "{$group: {_id: '$city', c: {$sum: 1}, p: {$sum: '$pop'}}}")
   * </code></p>
   *
   * @param mongoDb MongoDB connection
   * @param fields List of fields to project; or null to return map
   * @param operations One or more JSON strings
   * @return Enumerator of results
   */
  private Enumerable<Object> aggregate(final MongoDatabase mongoDb,
      final List<Map.Entry<String, Class>> fields,
      final List<String> operations) {
    final List<Bson> list = new ArrayList<>();
    for (String operation : operations) {
      list.add(BsonDocument.parse(operation));
    }
    final Function1<Document, Object> getter =
        MongoEnumerator.getter(fields);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final Iterator<Document> resultIterator;
        try {
          resultIterator = mongoDb.getCollection(collectionName)
              .aggregate(list).iterator();
        } catch (Exception e) {
          throw new RuntimeException("While running MongoDB query "
              + Util.toString(operations, "[", ",\n", "]"), e);
        }
        return new MongoEnumerator(resultIterator, getter);
      }
    };
  }

  /** Helper method to strip non-numerics from a string.
   *
   * <p>Currently used to determine mongod versioning numbers
   * from buildInfo.versionArray for use in aggregate method logic. */
  private static Integer parseIntString(String valueString) {
    return Integer.parseInt(valueString.replaceAll("[^0-9]", ""));
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.mongodb.MongoTable}.
   *
   * @param <T> element type */
  public static class MongoQueryable<T> extends AbstractTableQueryable<T> {
    MongoQueryable(QueryProvider queryProvider, SchemaPlus schema,
        MongoTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().find(getMongoDb(), null, null, null);
      return enumerable.enumerator();
    }

    private MongoDatabase getMongoDb() {
      return schema.unwrap(MongoSchema.class).mongoDb;
    }

    private MongoTable getTable() {
      return (MongoTable) table;
    }

    /** Called via code-generation.
     *
     * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_AGGREGATE
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> aggregate(List<Map.Entry<String, Class>> fields,
        List<String> operations) {
      return getTable().aggregate(getMongoDb(), fields, operations);
    }

    /** Called via code-generation.
     *
     * @param filterJson Filter document
     * @param projectJson Projection document
     * @param fields List of expected fields (and their types)
     * @return result of mongo query
     *
     * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_FIND
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(String filterJson,
        String projectJson, List<Map.Entry<String, Class>> fields) {
      return getTable().find(getMongoDb(), filterJson, projectJson, fields);
    }
  }
}

// End MongoTable.java
