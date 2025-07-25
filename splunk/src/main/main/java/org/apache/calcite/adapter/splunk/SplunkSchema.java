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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Schema for Splunk.
 * Supports both predefined CIM tables and custom table definitions.
 */
public class SplunkSchema extends AbstractSchema {
  public final SplunkConnection splunkConnection;
  private final Map<String, Table> predefinedTables;

  /**
   * Constructor for CIM model mode with predefined tables.
   *
   * @param connection Splunk connection
   * @param tables Map of table name to Table instances
   */
  public SplunkSchema(SplunkConnection connection, Map<String, Table> tables) {
    this.splunkConnection = connection;
    this.predefinedTables = tables;
  }

  /**
   * Constructor for custom table mode.
   * Tables will be defined via JSON table definitions.
   *
   * @param connection Splunk connection
   */
  public SplunkSchema(SplunkConnection connection) {
    this.splunkConnection = connection;
    this.predefinedTables = new HashMap<>();
  }

  @Override protected Map<String, Table> getTableMap() {
    // CIM model mode - return predefined tables
    return predefinedTables;
  }
}
