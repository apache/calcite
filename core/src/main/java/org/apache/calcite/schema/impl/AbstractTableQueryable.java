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
package org.apache.calcite.schema.impl;

import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Abstract implementation of {@link org.apache.calcite.linq4j.Queryable} for
 * {@link QueryableTable}.
 *
 * <p>Not to be confused with
 * {@link org.apache.calcite.adapter.java.AbstractQueryableTable}.</p>
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
