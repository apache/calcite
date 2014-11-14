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
package net.hydromatic.optiq.impl.clone;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Implementation of table that reads rows from a read-only list and returns
 * an enumerator of rows. Each row is object (if there is just one column) or
 * an object array (if there are multiple columns).
 */
class ListTable extends AbstractQueryableTable {
  private final RelProtoDataType protoRowType;
  private final Expression expression;
  private final List list;

  /** Creates a ListTable. */
  public ListTable(
      Type elementType,
      RelProtoDataType protoRowType,
      Expression expression,
      List list) {
    super(elementType);
    this.protoRowType = protoRowType;
    this.expression = expression;
    this.list = list;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public Statistic getStatistic() {
    return Statistics.of(list.size(), Collections.<BitSet>emptyList());
  }

  public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractQueryable<T>() {
      public Type getElementType() {
        return elementType;
      }

      public Expression getExpression() {
        return expression;
      }

      public QueryProvider getProvider() {
        return queryProvider;
      }

      public Iterator<T> iterator() {
        return Linq4j.enumeratorIterator(enumerator());
      }

      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return Linq4j.enumerator(list);
      }
    };
  }
}

// End ListTable.java
