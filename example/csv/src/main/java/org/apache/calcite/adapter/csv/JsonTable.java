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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.adapter.csv.JsonEnumerator.JsonDataConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractTable {
  private final Source source;
  protected List<Object> list = new ArrayList<>();

  public JsonTable(Source source) {
    this.source = source;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    JsonDataConverter jsonDataConverter = JsonEnumerator.deduceRowType(typeFactory, source);
    list = jsonDataConverter.getDataList();
    return jsonDataConverter.getRelDataType();
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }
}

// End JsonTable.java
