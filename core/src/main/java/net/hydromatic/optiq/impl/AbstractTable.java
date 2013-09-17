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
package net.hydromatic.optiq.impl;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Abstract base class for implementing {@link Table}.
 */
public abstract class AbstractTable<T>
    extends AbstractQueryable<T>
    implements Table<T>
{
  protected final Type elementType;
  private final RelDataType relDataType;
  protected final Schema schema;
  protected final String tableName;

  protected AbstractTable(
      Schema schema,
      Type elementType,
      RelDataType relDataType,
      String tableName) {
    this.schema = schema;
    this.elementType = elementType;
    this.relDataType = relDataType;
    this.tableName = tableName;
    assert schema != null;
    assert relDataType != null;
    assert elementType != null;
    assert tableName != null;
  }

  public QueryProvider getProvider() {
    return schema.getQueryProvider();
  }

  public DataContext getDataContext() {
    return schema;
  }

  // Default implementation. Override if you have statistics.
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public Type getElementType() {
    return elementType;
  }

  public RelDataType getRowType() {
    return relDataType;
  }

  public Expression getExpression() {
    return Expressions.call(
        schema.getExpression(),
        BuiltinMethod.DATA_CONTEXT_GET_TABLE.method,
        Expressions.<Expression>list()
            .append(Expressions.constant(tableName))
            .appendIf(
                elementType instanceof Class,
                Expressions.constant(elementType)));
  }

  public Iterator<T> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }
}

// End AbstractTable.java
