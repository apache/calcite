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
package net.hydromatic.optiq.impl.csv;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.Iterator;

/**
 * Table based on a CSV file.
 */
class CsvTable<T>
    extends AbstractQueryable<T>
    implements TranslatableTable<T>
{
  private final Type elementType;
  private final RelDataType rowType;
  final CsvSchema schema;
  private final String tableName;

  public CsvTable(
      Type elementType,
      RelDataType rowType,
      CsvSchema schema,
      String tableName)
  {
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
    return "CsvTable {" + tableName + "}";
  }

  public QueryProvider getProvider() {
    return schema.getQueryProvider();
  }

  public DataContext getDataContext() {
    return schema;
  }

  public Type getElementType() {
    return elementType;
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
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
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<T> enumerator() {
    return new Enumerator<T>() {
      public T current() {
        return null;
      }

      public boolean moveNext() {
        return false;
      }

      public void reset() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable)
  {
    return new CsvTableScan(
        context.getCluster(),
        relOptTable,
        this,
        RelOptUtil.getFieldNameList(relOptTable.getRowType()));
  }
}

// End CsvTable.java
