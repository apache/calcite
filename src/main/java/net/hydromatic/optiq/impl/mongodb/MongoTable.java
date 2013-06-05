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
package net.hydromatic.optiq.impl.mongodb;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.util.*;

/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryable<Object>
    implements TranslatableTable<Object> {
  private final MongoSchema schema;
  private final String tableName;
  private final RelDataType rowType;

  /** Creates a MongoTable. */
  MongoTable(MongoSchema schema, String tableName, RelDataType rowType) {
    this.schema = schema;
    this.tableName = tableName;
    this.rowType = rowType;
    assert rowType != null;
    assert schema != null;
    assert tableName != null;
  }

  public String toString() {
    return "MongoTable {" + tableName + "}";
  }

  public QueryProvider getProvider() {
    return schema.getQueryProvider();
  }

  public MongoSchema getDataContext() {
    return schema;
  }

  public Class getElementType() {
    return Object[].class;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public Expression getExpression() {
    return Expressions.convert_(
        Expressions.call(
            schema.getExpression(),
            "getTable",
            Expressions.<Expression>list()
                .append(Expressions.constant(tableName))
                .append(Expressions.constant(getElementType()))),
        MongoTable.class);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new MongoTableScan(context.getCluster(),
        context.getCluster().traitSetOf(MongoRel.CONVENTION), relOptTable,
        this, rowType, Collections.<Pair<String, String>>emptyList());
  }

  public Iterator<Object> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<Object> enumerator() {
    return find(null, null).enumerator();
  }

  /** Executes a "find" operation on the underlying collection.
   *
   * <p>For example,
   * <code>zipsTable.find("{state: 'OR'}", "{city: 1, zipcode: 1}")</code></p>
   *
   * @param filterJson Filter JSON string, or null
   * @param projectJson Project JSON string, or null
   * @return Enumerator of results
   */
  public Enumerable<Object> find(String filterJson, String projectJson) {
    final DBCollection collection = schema.mongoDb.getCollection(tableName);
    final DBObject filter =
        filterJson == null ? null : (DBObject) JSON.parse(filterJson);
    final DBObject project =
        projectJson == null ? null : (DBObject) JSON.parse(projectJson);
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final DBCursor cursor = collection.find(filter, project);
        return new MongoEnumerator(cursor);
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
   * @param operations One or more JSON strings
   * @return Enumerator of results
   */
  public Enumerable<Object> aggregate(String... operations) {
    List<DBObject> list = new ArrayList<DBObject>();
    for (String operation : operations) {
      list.add((DBObject) JSON.parse(operation));
    }
    final DBObject first = list.get(0);
    final List<DBObject> rest = list.subList(1, list.size());
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final AggregationOutput result =
            schema.mongoDb.getCollection(tableName)
                .aggregate(first, rest.toArray(new DBObject[rest.size()]));
        return new MongoEnumerator(result.results().iterator());
      }
    };
  }
}

// End MongoTable.java
