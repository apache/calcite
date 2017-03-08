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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.Map;

/**
 * Table implementation wrapping a URL / HTML table.
 */
class FileTable extends AbstractQueryableTable
    implements TranslatableTable {

  private final RelProtoDataType protoRowType;
  private FileReader reader;
  private FileRowConverter converter;

  /** Creates a FileTable. */
  private FileTable(Source source, String selector, Integer index,
      RelProtoDataType protoRowType, List<Map<String, Object>> fieldConfigs)
      throws Exception {
    super(Object[].class);

    this.protoRowType = protoRowType;
    this.reader = new FileReader(source, selector, index);
    this.converter = new FileRowConverter(this.reader, fieldConfigs);
  }

  /** Creates a FileTable. */
  static FileTable create(Source source, Map<String, Object> tableDef)
      throws Exception {
    @SuppressWarnings("unchecked") List<Map<String, Object>> fieldConfigs =
        (List<Map<String, Object>>) tableDef.get("fields");
    String selector = (String) tableDef.get("selector");
    Integer index = (Integer) tableDef.get("index");
    return new FileTable(source, selector, index, null, fieldConfigs);
  }

  public String toString() {
    return "FileTable";
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    return this.converter.getRowType((JavaTypeFactory) typeFactory);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        try {
          FileEnumerator enumerator =
              new FileEnumerator(reader.iterator(), converter);
          //noinspection unchecked
          return (Enumerator<T>) enumerator;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Returns an enumerable over a given projection of the fields. */
  public Enumerable<Object> project(final int[] fields) {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        try {
          return new FileEnumerator(reader.iterator(), converter, fields);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new EnumerableTableScan(context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        relOptTable, (Class) getElementType());
  }
}

// End FileTable.java
