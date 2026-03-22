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
package org.apache.calcite.adapter.kvrocks;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Metadata describing how a Kvrocks key maps to a relational table.
 *
 * <p>Carries the key name (used as Kvrocks key), the data format that
 * determines how stored values are parsed, the column definitions, and
 * an optional delimiter for CSV parsing.
 */
public class KvrocksTableFieldInfo {
  private String tableName;
  private String dataFormat;
  private List<LinkedHashMap<String, Object>> fields;
  private String keyDelimiter = ":";

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDataFormat() {
    return dataFormat;
  }

  public void setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat;
  }

  public List<LinkedHashMap<String, Object>> getFields() {
    return fields;
  }

  public void setFields(List<LinkedHashMap<String, Object>> fields) {
    this.fields = fields;
  }

  public String getKeyDelimiter() {
    return keyDelimiter;
  }

  public void setKeyDelimiter(String keyDelimiter) {
    this.keyDelimiter = keyDelimiter;
  }
}
