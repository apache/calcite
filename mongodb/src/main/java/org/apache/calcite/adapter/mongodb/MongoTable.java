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
import org.apache.calcite.runtime.ConsList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

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
  private Enumerable<Object> find(DB mongoDb, String filterJson,
      String projectJson, List<Map.Entry<String, Class>> fields) {
    final DBCollection collection =
        mongoDb.getCollection(collectionName);
    final DBObject filter =
        filterJson == null ? null : (DBObject) JSON.parse(filterJson);
    final DBObject project =
        projectJson == null ? null : (DBObject) JSON.parse(projectJson);
    final Function1<DBObject, Object> getter = MongoEnumerator.getter(fields);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final DBCursor cursor = collection.find(filter, project);
        return new MongoEnumerator(cursor, getter);
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
  private Enumerable<Object> aggregate(final DB mongoDb,
      final List<Map.Entry<String, Class>> fields,
      final List<String> operations) {
    final List<DBObject> list = new ArrayList<>();
    final BasicDBList versionArray = (BasicDBList) mongoDb
        .command("buildInfo").get("versionArray");
    final Integer versionMajor = parseIntString(versionArray
        .get(0).toString());
    final Integer versionMinor = parseIntString(versionArray
        .get(1).toString());
//    final Integer versionMaintenance = parseIntString(versionArray
//      .get(2).toString());
//    final Integer versionBuild = parseIntString(versionArray
//      .get(3).toString());

    for (String operation : operations) {
      list.add((DBObject) JSON.parse(operation));
    }
    final DBObject first = list.get(0);
    final List<DBObject> rest = Util.skip(list);
    final Function1<DBObject, Object> getter =
        MongoEnumerator.getter(fields);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final Iterator<DBObject> resultIterator;
        try {
          // Changed in version 2.6: The db.collection.aggregate() method
          // returns a cursor
          // and can return result sets of any size.
          // See: http://docs.mongodb.org/manual/core/aggregation-pipeline
          if (versionMajor > 1) {
            // MongoDB version 2.6+
            if (versionMinor > 5) {
              AggregationOptions options = AggregationOptions.builder()
                   .outputMode(AggregationOptions.OutputMode.CURSOR).build();
              // Warning - this can result in a very large ArrayList!
              // but you should know your data and aggregate accordingly
              final List<DBObject> resultAsArrayList =
                  Lists.newArrayList(mongoDb.getCollection(collectionName)
                      .aggregate(list, options));
              resultIterator = resultAsArrayList.iterator();
            } else { // Pre MongoDB version 2.6
              AggregationOutput result = aggregateOldWay(mongoDb
                   .getCollection(collectionName), first, rest);
              resultIterator = result.results().iterator();
            }
          } else { // Pre MongoDB version 2
            AggregationOutput result = aggregateOldWay(mongoDb
                 .getCollection(collectionName), first, rest);
            resultIterator = result.results().iterator();
          }
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

  /** Executes an "aggregate" operation for pre-2.6 mongo servers.
   *
   * <p>Return document is limited to 4M or 16M in size depending on
   * version of mongo.

   * <p>Helper method for
   * {@link org.apache.calcite.adapter.mongodb.MongoTable#aggregate}.
   *
   * @param dbCollection Collection
   * @param first First aggregate action
   * @param rest Rest of the aggregate actions
   * @return Aggregation output
   */
  private AggregationOutput aggregateOldWay(DBCollection dbCollection,
       DBObject first, List<DBObject> rest) {
    return dbCollection.aggregate(ConsList.of(first, rest));
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.mongodb.MongoTable}. */
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

    private DB getMongoDb() {
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
