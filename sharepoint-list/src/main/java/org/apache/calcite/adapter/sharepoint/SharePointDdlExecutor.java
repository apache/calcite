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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Executes DDL commands for SharePoint lists.
 * Supports CREATE TABLE (creates SharePoint list) and DROP TABLE (deletes SharePoint list).
 */
public class SharePointDdlExecutor extends DdlExecutorImpl {
  
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  /** Executes a {@code CREATE TABLE} command to create a SharePoint list. */
  public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = schema(context, true, create.name);
    final CalciteSchema schema = pair.left;
    final String listName = pair.right;
    
    // Get the SharePoint schema
    SharePointListSchema spSchema = getSharePointSchema(schema);
    
    try {
      // Build column definitions from the CREATE TABLE statement
      List<SharePointColumn> columns = new ArrayList<>();
      
      if (create.columnList != null) {
        for (SqlNode node : create.columnList) {
          if (node instanceof SqlColumnDeclaration) {
            SqlColumnDeclaration col = (SqlColumnDeclaration) node;
            String columnName = col.name.getSimple();
            
            // For now, use a simple type mapping
            // In a full implementation, we'd derive the type from col.dataType
            String spFieldType = "text"; // Default to text
            
            // Check for common type patterns in the column declaration
            if (col.dataType != null && col.dataType.toString() != null) {
              String typeStr = col.dataType.toString().toUpperCase();
              if (typeStr.contains("INT")) {
                spFieldType = "number";
              } else if (typeStr.contains("BOOL")) {
                spFieldType = "boolean";
              } else if (typeStr.contains("DATE") || typeStr.contains("TIME")) {
                spFieldType = "dateTime";
              }
            }
            
            columns.add(new SharePointColumn(columnName, spFieldType, false));
          }
        }
      }
      
      // Create the SharePoint list
      createSharePointList(spSchema, listName, columns);
      
      // Refresh the schema to include the new list
      spSchema.refresh();
      
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SharePoint list: " + listName, e);
    }
  }
  
  /** Executes a {@code DROP TABLE} command to delete a SharePoint list. */
  public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = schema(context, true, drop.name);
    final CalciteSchema schema = pair.left;
    final String listName = pair.right;
    
    // Get the SharePoint schema
    SharePointListSchema spSchema = getSharePointSchema(schema);
    
    try {
      // Delete the SharePoint list
      deleteSharePointList(spSchema, listName, drop.ifExists);
      
      // Refresh the schema to remove the deleted list
      spSchema.refresh();
      
    } catch (Exception e) {
      if (!drop.ifExists) {
        throw new RuntimeException("Failed to drop SharePoint list: " + listName, e);
      }
    }
  }
  
  private SharePointListSchema getSharePointSchema(CalciteSchema schema) {
    Schema rawSchema = schema.schema;
    if (rawSchema instanceof SharePointListSchema) {
      return (SharePointListSchema) rawSchema;
    }
    // Try to find SharePoint schema in sub-schemas
    for (CalciteSchema subSchema : schema.getSubSchemaMap().values()) {
      if (subSchema.schema instanceof SharePointListSchema) {
        return (SharePointListSchema) subSchema.schema;
      }
    }
    throw new IllegalStateException("No SharePoint schema found");
  }
  
  private void createSharePointList(SharePointListSchema schema, String listName, 
      List<SharePointColumn> columns) throws IOException, InterruptedException {
    
    if (schema.useRestApi()) {
      // Use SharePoint REST API
      createSharePointListRest(schema, listName, columns);
    } else {
      // Use Microsoft Graph API
      createSharePointListGraph(schema, listName, columns);
    }
  }
  
  private void createSharePointListGraph(SharePointListSchema schema, String listName,
      List<SharePointColumn> columns) throws IOException, InterruptedException {
    
    MicrosoftGraphListClient client = schema.getClient();
    
    // Build the JSON request body for creating a list
    ObjectNode requestBody = MAPPER.createObjectNode();
    requestBody.put("displayName", listName);
    requestBody.put("description", "Created by Apache Calcite SharePoint Adapter");
    
    // Create list with basic template (Generic List = 100)
    ObjectNode list = MAPPER.createObjectNode();
    list.put("template", "genericList");
    requestBody.set("list", list);
    
    // Create the list first
    String createListUrl = String.format("%s/sites/%s/lists",
        client.getGraphApiBase(), client.getSiteId());
    
    JsonNode response = client.executeGraphCall("POST", createListUrl, requestBody);
    String listId = response.get("id").asText();
    
    // Now add columns to the list
    for (SharePointColumn column : columns) {
      createColumnGraph(client, listId, column);
    }
  }
  
  private void createSharePointListRest(SharePointListSchema schema, String listName,
      List<SharePointColumn> columns) throws IOException, InterruptedException {
    
    SharePointRestListClient restClient = schema.getRestClient();
    String siteUrl = restClient.getSiteUrl();
    
    // Build the JSON request body for creating a list via REST API
    ObjectNode requestBody = MAPPER.createObjectNode();
    ObjectNode metadata = MAPPER.createObjectNode();
    metadata.put("type", "SP.List");
    requestBody.set("__metadata", metadata);
    requestBody.put("Title", listName);
    requestBody.put("Description", "Created by Apache Calcite SharePoint Adapter");
    requestBody.put("BaseTemplate", 100); // Generic List template
    requestBody.put("EnableAttachments", true);
    
    // Create the list using REST API
    String createListUrl = siteUrl + "/_api/web/lists";
    JsonNode response = restClient.executeRestCall("POST", createListUrl, requestBody);
    String listId = response.get("d").get("Id").asText();
    
    // Add columns to the list
    for (SharePointColumn column : columns) {
      createColumnRest(restClient, listName, column);
    }
  }
  
  private void createColumnGraph(MicrosoftGraphListClient client, String listId, 
      SharePointColumn column) throws IOException, InterruptedException {
    
    String createColumnUrl = String.format("%s/sites/%s/lists/%s/columns",
        client.getGraphApiBase(), client.getSiteId(), listId);
    
    ObjectNode requestBody = MAPPER.createObjectNode();
    requestBody.put("name", column.name);
    requestBody.put("displayName", column.name);
    requestBody.put("enforceUniqueValues", false);
    requestBody.put("required", column.required);
    
    // Set the column type based on SharePoint field type
    switch (column.fieldType) {
      case "text":
        ObjectNode textDef = MAPPER.createObjectNode();
        textDef.put("maxLength", 255);
        requestBody.set("text", textDef);
        break;
      
      case "number":
        ObjectNode numberDef = MAPPER.createObjectNode();
        requestBody.set("number", numberDef);
        break;
      
      case "boolean":
        ObjectNode boolDef = MAPPER.createObjectNode();
        requestBody.set("boolean", boolDef);
        break;
      
      case "dateTime":
        ObjectNode dateDef = MAPPER.createObjectNode();
        dateDef.put("format", "dateTime");
        requestBody.set("dateTime", dateDef);
        break;
      
      default:
        // Default to text
        ObjectNode defaultText = MAPPER.createObjectNode();
        defaultText.put("maxLength", 255);
        requestBody.set("text", defaultText);
    }
    
    client.executeGraphCall("POST", createColumnUrl, requestBody);
  }
  
  private void createColumnRest(SharePointRestListClient restClient, String listName,
      SharePointColumn column) throws IOException, InterruptedException {
    
    String siteUrl = restClient.getSiteUrl();
    String createColumnUrl = siteUrl + "/_api/web/lists/getbytitle('" + 
        listName.replace("'", "''") + "')/fields";
    
    ObjectNode requestBody = MAPPER.createObjectNode();
    ObjectNode metadata = MAPPER.createObjectNode();
    metadata.put("type", "SP.Field");
    requestBody.set("__metadata", metadata);
    requestBody.put("Title", column.name);
    requestBody.put("FieldTypeKind", mapFieldTypeToRestApi(column.fieldType));
    requestBody.put("Required", column.required);
    
    restClient.executeRestCall("POST", createColumnUrl, requestBody);
  }
  
  private int mapFieldTypeToRestApi(String fieldType) {
    // SharePoint REST API field type kinds
    switch (fieldType) {
      case "text":
        return 2; // Text
      case "number":
        return 9; // Number
      case "boolean":
        return 8; // Boolean
      case "dateTime":
        return 4; // DateTime
      default:
        return 2; // Default to Text
    }
  }
  
  private void deleteSharePointList(SharePointListSchema schema, String listName, 
      boolean ifExists) throws IOException, InterruptedException {
    
    if (schema.useRestApi()) {
      // Use SharePoint REST API
      deleteSharePointListRest(schema, listName, ifExists);
    } else {
      // Use Microsoft Graph API
      deleteSharePointListGraph(schema, listName, ifExists);
    }
  }
  
  private void deleteSharePointListGraph(SharePointListSchema schema, String listName,
      boolean ifExists) throws IOException, InterruptedException {
    
    MicrosoftGraphListClient client = schema.getClient();
    
    // Find the list ID by name
    Map<String, SharePointListMetadata> lists = client.getAvailableLists();
    String listId = null;
    
    for (SharePointListMetadata metadata : lists.values()) {
      if (metadata.getListName().equalsIgnoreCase(listName) ||
          metadata.getDisplayName().equalsIgnoreCase(listName)) {
        listId = metadata.getListId();
        break;
      }
    }
    
    if (listId == null) {
      if (!ifExists) {
        throw new RuntimeException("List not found: " + listName);
      }
      return;
    }
    
    // Delete the list
    String deleteUrl = String.format("%s/sites/%s/lists/%s",
        client.getGraphApiBase(), client.getSiteId(), listId);
    
    client.executeGraphCall("DELETE", deleteUrl, null);
  }
  
  private void deleteSharePointListRest(SharePointListSchema schema, String listName,
      boolean ifExists) throws IOException, InterruptedException {
    
    SharePointRestListClient restClient = schema.getRestClient();
    String siteUrl = restClient.getSiteUrl();
    
    try {
      // Delete the list using REST API
      String deleteUrl = siteUrl + "/_api/web/lists/getbytitle('" + 
          listName.replace("'", "''") + "')";
      
      restClient.executeRestCall("DELETE", deleteUrl, null);
    } catch (Exception e) {
      if (!ifExists) {
        throw new RuntimeException("Failed to delete list: " + listName, e);
      }
    }
  }
  
  private String mapSqlTypeToSharePoint(SqlTypeName sqlType) {
    switch (sqlType) {
      case VARCHAR:
      case CHAR:
        return "text";
      
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case REAL:
        return "number";
      
      case BOOLEAN:
        return "boolean";
      
      case DATE:
      case TIME:
      case TIMESTAMP:
        return "dateTime";
      
      case BINARY:
      case VARBINARY:
        return "text"; // SharePoint doesn't have direct binary columns
      
      default:
        return "text";
    }
  }
  
  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
      boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema =
        mutable ? context.getMutableRootSchema()
            : context.getRootSchema();
    for (String p : path) {
      schema = requireNonNull(schema.getSubSchema(p, true));
    }
    return Pair.of(schema, name);
  }
  
  /** Helper class to represent a SharePoint column definition. */
  private static class SharePointColumn {
    final String name;
    final String fieldType;
    final boolean required;
    
    SharePointColumn(String name, String fieldType, boolean required) {
      this.name = name;
      this.fieldType = fieldType;
      this.required = required;
    }
  }
}