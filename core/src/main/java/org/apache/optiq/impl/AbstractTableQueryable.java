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
package org.apache.optiq.impl;

import org.apache.linq4j.*;
import org.apache.linq4j.expressions.Expression;

import org.apache.optiq.QueryableTable;
import org.apache.optiq.SchemaPlus;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Abstract implementation of {@link org.apache.linq4j.Queryable} for
 * {@link QueryableTable}.
 *
 * <p>Not to be confused with
 * {@link org.apache.optiq.impl.enumerable.AbstractQueryableTable}.</p>
 *
 * @param <T> element type
 */
public abstract class AbstractTableQueryable<T> extends AbstractQueryable<T> {
  public final QueryProvider queryProvider;
  public final SchemaPlus schema;
  public final QueryableTable table;
  public final String tableName;

  public AbstractTableQueryable(QueryProvider queryProvider,
      SchemaPlus schema, QueryableTable table, String tableName) {
    this.queryProvider = queryProvider;
    this.schema = schema;
    this.table = table;
    this.tableName = tableName;
  }

  public Expression getExpression() {
    return table.getExpression(schema, tableName, Queryable.class);
  }

  public QueryProvider getProvider() {
    return queryProvider;
  }

  public Type getElementType() {
    return table.getElementType();
  }

  public Iterator<T> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }
}

// End AbstractTableQueryable.java
