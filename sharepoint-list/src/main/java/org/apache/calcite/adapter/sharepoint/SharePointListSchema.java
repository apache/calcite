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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;
import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Schema implementation for SharePoint Lists.
 */
public class SharePointListSchema extends AbstractSchema {
  private final String siteUrl;
  private final Map<String, Table> tableMap;
  private final SharePointAuth authenticator;

  public SharePointListSchema(String siteUrl, Map<String, Object> authConfig) {
    this.siteUrl = siteUrl;
    this.authenticator = SharePointAuthFactory.createAuth(authConfig);
    this.tableMap = createTableMap();
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
      SharePointRestClient client = new SharePointRestClient(siteUrl, authenticator);
      Map<String, SharePointListMetadata> lists = client.getAvailableLists();

      for (Map.Entry<String, SharePointListMetadata> entry : lists.entrySet()) {
        String tableName = entry.getKey();
        SharePointListMetadata metadata = entry.getValue();
        builder.put(tableName, new SharePointListTable(metadata, client));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to SharePoint", e);
    }

    return builder.build();
  }
}
