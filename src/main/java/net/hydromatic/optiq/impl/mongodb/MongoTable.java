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
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;

import java.util.Iterator;

/**
 * Table based on a MongoDB collection.
 */
public class MongoTable extends AbstractQueryable<Object[]>
    implements TranslatableTable<Object[]> {
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

  public Iterator<Object[]> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<Object[]> enumerator() {
    return new MongoEnumerator(schema.mongoDb, tableName);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable)
  {
    return new JavaRules.EnumerableTableAccessRel(
        context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.CUSTOM),
        relOptTable,
        getExpression(),
        getElementType());
  }
}

// End MongoTable.java
