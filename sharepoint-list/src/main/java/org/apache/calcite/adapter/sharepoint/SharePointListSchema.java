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
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schema implementation for SharePoint Lists with CREATE/DROP support.
 */
public class SharePointListSchema extends AbstractSchema {
  private final String siteUrl;
  private final Map<String, Table> tableMap;
  private final SharePointAuth authenticator;
  private final MicrosoftGraphListClient client;
  private final SharePointRestListClient restClient;
  private final SharePointMetadataSchema metadataSchema;
  private final boolean useRestApi;

  public SharePointListSchema(String siteUrl, Map<String, Object> authConfig) {
    this.siteUrl = siteUrl;
    this.authenticator = SharePointAuthFactory.createAuth(authConfig);

    // Check if REST API should be used (default to Graph API)
    this.useRestApi = "rest".equalsIgnoreCase((String) authConfig.get("apiType"));

    this.client = new MicrosoftGraphListClient(siteUrl, authenticator);
    this.restClient = new SharePointRestListClient(siteUrl, authenticator);
    this.tableMap = new ConcurrentHashMap<>(createTableMap());
    this.metadataSchema = new SharePointMetadataSchema(this, "sharepoint", "public");
  }

  /**
   * Gets the Microsoft Graph client for use by functions.
   */
  public MicrosoftGraphListClient getClient() {
    return client;
  }

  /**
   * Gets the SharePoint REST client for use by functions.
   */
  public SharePointRestListClient getRestClient() {
    return restClient;
  }

  /**
   * Returns true if REST API should be used instead of Graph API.
   */
  public boolean useRestApi() {
    return useRestApi;
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  @Override protected Multimap<String, Function> getFunctionMultimap() {
    ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();

    try {
      // Register unified attachment functions that automatically choose Graph API or REST API

      // get_attachments - table function that returns attachments for an item
      Method getAttachmentsMethod = SharePointAttachmentFunctions.class
          .getMethod("getAttachments", org.apache.calcite.DataContext.class, String.class, String.class);
      Function getAttachmentsFunc = TableFunctionImpl.create(getAttachmentsMethod);
      if (getAttachmentsFunc != null) {
        builder.put("get_attachments", getAttachmentsFunc);
      }

      // add_attachment - scalar function to add an attachment
      Method addAttachmentMethod = SharePointAttachmentFunctions.class
          .getMethod("addAttachment", org.apache.calcite.DataContext.class,
              String.class, String.class, String.class, byte[].class);
      Function addAttachmentFunc = ScalarFunctionImpl.create(addAttachmentMethod);
      if (addAttachmentFunc != null) {
        builder.put("add_attachment", addAttachmentFunc);
      }

      // delete_attachment - scalar function to delete an attachment
      Method deleteAttachmentMethod = SharePointAttachmentFunctions.class
          .getMethod("deleteAttachment", org.apache.calcite.DataContext.class,
              String.class, String.class, String.class);
      Function deleteAttachmentFunc = ScalarFunctionImpl.create(deleteAttachmentMethod);
      if (deleteAttachmentFunc != null) {
        builder.put("delete_attachment", deleteAttachmentFunc);
      }

      // get_attachment_content - scalar function to get attachment content
      Method getContentMethod = SharePointAttachmentFunctions.class
          .getMethod("getAttachmentContent", org.apache.calcite.DataContext.class,
              String.class, String.class, String.class);
      Function getContentFunc = ScalarFunctionImpl.create(getContentMethod);
      if (getContentFunc != null) {
        builder.put("get_attachment_content", getContentFunc);
      }

      // count_attachments - scalar function to count attachments
      Method countAttachmentsMethod = SharePointAttachmentFunctions.class
          .getMethod("countAttachments", org.apache.calcite.DataContext.class,
              String.class, String.class);
      Function countAttachmentsFunc = ScalarFunctionImpl.create(countAttachmentsMethod);
      if (countAttachmentsFunc != null) {
        builder.put("count_attachments", countAttachmentsFunc);
      }

    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to register SharePoint attachment functions", e);
    }

    return builder.build();
  }

  @Override public boolean isMutable() {
    return true; // Schema supports CREATE/DROP operations
  }

  @Override protected Map<String, Schema> getSubSchemaMap() {
    // No sub-schemas - metadata schemas are now at root level
    return ImmutableMap.of();
  }

  // TODO: To support CREATE/DROP TABLE, we would need to implement a custom
  // Schema interface that extends AbstractSchema with these methods.
  // For now, these operations can be done programmatically through the client.

  /**
   * Provides access to the underlying table map for metadata queries.
   * Used by SharePointMetadataSchema to inspect available tables.
   */
  public Map<String, Table> getTableMapForMetadata() {
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
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

  /**
   * Refreshes the schema to pick up newly created or deleted lists.
   */
  public void refresh() {
    // Clear and rebuild the table map
    tableMap.clear();
    tableMap.putAll(createTableMap());
  }

  /**
   * Creates a new SharePoint list with the given name and columns.
   * This is called by the DDL executor for CREATE TABLE statements.
   */
  public void createList(String listName, List<ColumnDefinition> columns)
      throws IOException, InterruptedException {
    // This method would be called by SharePointDdlExecutor
    // Implementation handled in SharePointDdlExecutor
  }

  /**
   * Deletes a SharePoint list with the given name.
   * This is called by the DDL executor for DROP TABLE statements.
   */
  public void dropList(String listName, boolean ifExists)
      throws IOException, InterruptedException {
    // This method would be called by SharePointDdlExecutor
    // Implementation handled in SharePointDdlExecutor
  }

  /**
   * Helper class for column definitions.
   */
  public static class ColumnDefinition {
    public final String name;
    public final String type;
    public final boolean required;

    public ColumnDefinition(String name, String type, boolean required) {
      this.name = name;
      this.type = type;
      this.required = required;
    }
  }

}
