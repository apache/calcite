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
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Table based on Splunk.
 */
class SplunkTable<T>
    extends AbstractQueryable<T>
    implements TranslatableTable<T> {
  private final Type elementType;
  private final RelDataType rowType;
  final SplunkSchema schema;
  private final String tableName;

  public SplunkTable(
      Type elementType,
      RelDataType rowType,
      SplunkSchema schema,
      String tableName) {
    this.elementType = elementType;
    this.rowType = rowType;
    this.schema = schema;
    this.tableName = tableName;
    assert elementType != null;
    assert rowType != null;
    assert schema != null;
    assert tableName != null;
  }

  public String toString() {
    return "SplunkTable {" + tableName + "}";
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public QueryProvider getProvider() {
    return schema.queryProvider;
  }

  public Type getElementType() {
    return elementType;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Expression getExpression() {
    return Expressions.call(
        schema.getExpression(),
        "getTable",
        Expressions.<Expression>list()
            .append(Expressions.constant(tableName))
            .appendIf(
                elementType instanceof Class,
                Expressions.constant(elementType)));
  }

  public Iterator<T> iterator() {
    return createQuery().iterator();
  }

  public Enumerator<T> enumerator() {
    return createQuery().enumerator();
  }

  private SplunkQuery<T> createQuery() {
    return new SplunkQuery<T>(
        schema.splunkConnection,
        "search",
        null,
        null,
        null);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new SplunkTableAccessRel(
        context.getCluster(),
        relOptTable,
        this,
        "search",
        null,
        null,
        relOptTable.getRowType().getFieldNames());
  }
}

// End SplunkTable.java
