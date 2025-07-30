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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

/**
 * Factory for creating SharePoint list tables.
 */
public class SharePointListTableFactory implements TableFactory<Table> {

  public static final SharePointListTableFactory INSTANCE = new SharePointListTableFactory();

  private SharePointListTableFactory() {
  }

  @Override public Table create(SchemaPlus schema, String name, Map<String, Object> operand,
      RelDataType rowType) {
    String siteUrl = (String) operand.get("siteUrl");
    String listId = (String) operand.get("listId");
    String listName = (String) operand.get("listName");

    if (siteUrl == null) {
      throw new RuntimeException("siteUrl is required");
    }

    if (listId == null && listName == null) {
      throw new RuntimeException("Either listId or listName is required");
    }

    try {
      // Extract auth config from operand
      Map<String, Object> authConfig = new java.util.HashMap<>();
      authConfig.put("authType", operand.get("authType"));
      authConfig.put("clientId", operand.get("clientId"));
      authConfig.put("clientSecret", operand.get("clientSecret"));
      authConfig.put("tenantId", operand.get("tenantId"));
      authConfig.put("username", operand.get("username"));
      authConfig.put("password", operand.get("password"));
      authConfig.put("certificatePath", operand.get("certificatePath"));
      authConfig.put("certificatePassword", operand.get("certificatePassword"));
      authConfig.put("thumbprint", operand.get("thumbprint"));

      SharePointAuth authenticator = SharePointAuthFactory.createAuth(authConfig);
      SharePointRestClient client = new SharePointRestClient(siteUrl, authenticator);

      SharePointListMetadata metadata;
      if (listId != null) {
        metadata = client.getListMetadataById(listId);
      } else {
        metadata = client.getListMetadataByName(listName);
      }

      return new SharePointListTable(metadata, client);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SharePoint table", e);
    }
  }
}
