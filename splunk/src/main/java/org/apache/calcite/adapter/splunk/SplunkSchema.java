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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Splunk schema.
 */
public class SplunkSchema extends AbstractSchema {
  /** The name of the one and only table. */
  public static final String SPLUNK_TABLE_NAME = "splunk";

  public static final ImmutableMap<String, Table> TABLE_MAP =
      ImmutableMap.of(SPLUNK_TABLE_NAME, SplunkTable.INSTANCE);

  public final SplunkConnection splunkConnection;

  /** Creates a SplunkSchema. */
  public SplunkSchema(SplunkConnection splunkConnection) {
    super();
    this.splunkConnection = splunkConnection;
  }

  @Override protected Map<String, Table> getTableMap() {
    return TABLE_MAP;
  }
}
