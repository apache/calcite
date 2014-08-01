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
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.util.*;

/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryableTable
    implements TranslatableTable {
  protected final String collectionName;

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
            typeFactory.createSqlType(SqlTypeName.ANY));
    return typeFactory.builder().add("_MAP", mapType).build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new MongoQueryable<T>(queryProvider, schema, this, tableName);
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
  public Enumerable<Object> find(DB mongoDb, String filterJson,
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
  public Enumerable<Object> aggregate(final DB mongoDb,
      final List<Map.Entry<String, Class>> fields,
      final List<String> operations) {
    final List<DBObject> list = new ArrayList<DBObject>();
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
              ArrayList<DBObject> resultAsArrayList
                = new ArrayList<DBObject>(Util.toList(mongoDb.
                      getCollection(collectionName)
                       .aggregate(list, options)));
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

  /** Helper method to strip non-numerics from a string
   * <p>Currently used to determine mongod versioning numbers
   * from buildInfo.versionArray for use in aggregate method logic</p>
   * @param valueString
   * @return Integer */
  private static Integer parseIntString(String valueString) {
    return Integer.parseInt(valueString.replaceAll("[^0-9]", ""));
  }

  /** Executes an "aggregate" operation for pre-2.6 mongo servers.
   * <p>Return document is limited to 4M or 16M in size depending on
   * version of mongo <p>Helper method for
   * {@link net.hydromatic.optiq.impl.mongodb.MongoTable#aggregate}
   * </p>
   * @param dbCollection
   * @param first the first aggregate action
   * @param rest the rest of the aggregate actions
   * @return AggregationOutput */
  private AggregationOutput aggregateOldWay(DBCollection dbCollection,
       DBObject first, List<DBObject> rest) {
    return dbCollection.aggregate(first, rest
          .toArray(new DBObject[rest.size()]));
  }

  /** Implementation of {@link net.hydromatic.linq4j.Queryable} based on
   * a {@link net.hydromatic.optiq.impl.mongodb.MongoTable}. */
  public static class MongoQueryable<T> extends AbstractTableQueryable<T> {
    public MongoQueryable(QueryProvider queryProvider, SchemaPlus schema,
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
     * @see net.hydromatic.optiq.impl.mongodb.MongoMethod#MONGO_QUERYABLE_AGGREGATE
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> aggregate(List<Map.Entry<String, Class>> fields,
        List<String> operations) {
      return getTable().aggregate(getMongoDb(), fields, operations);
    }

    /** Called via code-generation.
     *
     * @see net.hydromatic.optiq.impl.mongodb.MongoMethod#MONGO_QUERYABLE_FIND
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> find(String filterJson,
        String projectJson, List<Map.Entry<String, Class>> fields) {
      return getTable().find(getMongoDb(), filterJson, projectJson, fields);
    }
  }
}

// End MongoTable.java
