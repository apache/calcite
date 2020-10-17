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

import org.apache.calcite.adapter.file.JsonEnumerator.JsonDataConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.List;

/**
 * Table based on a JSON file.
 *
 * 基于 json 格式的文件。
 */
public class JsonTable extends AbstractTable {
  // 数据源
  private final Source source;

  private RelDataType rowType;
  protected List<Object> dataList;

  public JsonTable(Source source) {
    this.source = source;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType == null) {
      rowType = JsonEnumerator.deduceRowType(typeFactory, source).getRelDataType();
    }
    return rowType;
  }

  /**
   * Returns the data list of the table.
   */
  public List<Object> getDataList(RelDataTypeFactory typeFactory) {
    if (dataList == null) {
      JsonDataConverter jsonDataConverter =
          JsonEnumerator.deduceRowType(typeFactory, source);
      dataList = jsonDataConverter.getDataList();
    }
    return dataList;
  }

  @Override public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }
}
