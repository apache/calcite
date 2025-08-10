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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.linq4j.CsvEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scannable table based on a CSV file.
 * Used for Parquet conversion where we need to scan the table data.
 */
public class CsvScannableTable extends CsvTable implements ScannableTable {

  /** Creates a CsvScannableTable. */
  public CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType) {
    super(source, protoRowType);
  }

  /** Creates a CsvScannableTable with column casing. */
  public CsvScannableTable(Source source, @Nullable RelProtoDataType protoRowType, String columnCasing) {
    super(source, protoRowType, columnCasing);
  }

  @Override public String toString() {
    return "CsvScannableTable";
  }

  @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
    JavaTypeFactory typeFactory = root.getTypeFactory();
    final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
    final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<@Nullable Object[]>() {
      @Override public Enumerator<@Nullable Object[]> enumerator() {
        return new CsvEnumerator<>(source, cancelFlag, false, null,
            CsvEnumerator.arrayConverter(fieldTypes, fields, false));
      }
    };
  }
}
