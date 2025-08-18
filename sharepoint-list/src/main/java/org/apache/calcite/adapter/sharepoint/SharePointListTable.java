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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Table implementation for SharePoint lists with full CRUD support.
 */
public class SharePointListTable extends AbstractQueryableTable
    implements ScannableTable, ModifiableTable {
  private final SharePointListMetadata metadata;
  private final MicrosoftGraphListClient client;

  public SharePointListTable(SharePointListMetadata metadata, MicrosoftGraphListClient client) {
    super(Object[].class);
    this.metadata = metadata;
    this.client = client;
  }

  public SharePointListMetadata getMetadata() {
    return metadata;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();

    // Add ID column first (always present in SharePoint lists, but nullable for inserts)
    names.add("id");
    types.add(
        typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), true));

    // Track column names to avoid duplicates
    java.util.Set<String> seenNames = new HashSet<>();
    seenNames.add("id");

    for (SharePointColumn column : metadata.getColumns()) {
      String columnName = column.getName();
      // Skip if we already have this column name
      if (!seenNames.contains(columnName)) {
        names.add(columnName);
        seenNames.add(columnName);

        SqlTypeName sqlType = mapSharePointTypeToSql(column.getType());
        // Make column nullable unless it's required
        RelDataType columnType = column.isRequired()
            ? typeFactory.createSqlType(sqlType)
            : typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true);
        types.add(columnType);
      }
    }

    return typeFactory.createStructType(types, names);
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new SharePointListEnumerator(metadata, client);
      }
    };
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      @Override public Enumerator<T> enumerator() {
        @SuppressWarnings("unchecked")
        Enumerator<T> enumerator = (Enumerator<T>) new SharePointListEnumerator(metadata, client);
        return enumerator;
      }
    };
  }

  @Override public @Nullable Collection getModifiableCollection() {
    return new SharePointModifiableCollection();
  }

  @Override public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child,
      TableModify.Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    return LogicalTableModify.create(table, catalogReader, child, operation,
        updateColumnList, sourceExpressionList, flattened);
  }

  /**
   * Modifiable collection implementation for SharePoint lists.
   * This acts as a bridge between Calcite's collection-based operations
   * and SharePoint's REST API.
   */
  private class SharePointModifiableCollection extends ArrayList<Object[]> {

    @Override public boolean add(Object[] row) {
      try {
        // Convert row to SharePoint fields, skipping null ID
        Map<String, Object> fields = convertRowToFields(row);

        // Create item in SharePoint
        String itemId = client.createListItem(metadata.getListId(), fields);

        if (itemId != null) {
          // Update the row with the generated ID
          row[0] = itemId;
          // Add to local collection for consistency
          super.add(row);
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException("Failed to insert row into SharePoint list: " +
            e.getMessage(), e);
      }
    }

    @Override public boolean addAll(Collection<? extends Object[]> rows) {
      // For batch operations, we need to insert one by one to get IDs
      boolean allSucceeded = true;
      for (Object[] row : rows) {
        if (!add(row)) {
          allSucceeded = false;
        }
      }
      return allSucceeded;
    }

    @Override public boolean remove(Object o) {
      if (!(o instanceof Object[])) {
        return false;
      }

      Object[] row = (Object[]) o;
      if (row.length == 0 || row[0] == null) {
        return false;
      }

      try {
        String itemId = row[0].toString();
        client.deleteListItem(metadata.getListId(), itemId);
        // Remove from local collection
        return super.remove(o);
      } catch (Exception e) {
        throw new RuntimeException("Failed to delete row from SharePoint list: " +
            e.getMessage(), e);
      }
    }

    @Override public void clear() {
      try {
        List<Map<String, Object>> items = client.getListItems(metadata.getListId());
        for (Map<String, Object> item : items) {
          String itemId = item.get("id").toString();
          client.deleteListItem(metadata.getListId(), itemId);
        }
        super.clear();
      } catch (Exception e) {
        throw new RuntimeException("Failed to clear SharePoint list: " + e.getMessage(), e);
      }
    }

    @Override public Object[] set(int index, Object[] element) {
      if (element == null || element.length == 0 || element[0] == null) {
        throw new IllegalArgumentException("Cannot update row without ID");
      }

      try {
        String itemId = element[0].toString();
        
        // Convert row to fields map, excluding the ID
        Map<String, Object> fields = convertRowToFieldsForUpdate(element);
        
        // Update via SharePoint API
        client.updateListItem(metadata.getListId(), itemId, fields);
        
        // Update local collection
        return super.set(index, element);
      } catch (Exception e) {
        throw new RuntimeException("Failed to update row in SharePoint list: " +
            e.getMessage(), e);
      }
    }

    /**
     * Converts a row array to SharePoint field map.
     */
    private Map<String, Object> convertRowToFields(Object[] row) {
      Map<String, Object> fields = new LinkedHashMap<>();

      // Skip the ID field (index 0) for inserts - SharePoint assigns IDs automatically
      List<SharePointColumn> columns = metadata.getColumns();

      for (int i = 0; i < columns.size() && i + 1 < row.length; i++) {
        SharePointColumn column = columns.get(i);
        Object value = row[i + 1]; // +1 to skip ID column

        if (value != null) {
          fields.put(column.getInternalName(), value);
        }
      }

      return fields;
    }

    /**
     * Converts a row array to SharePoint field map for updates.
     * Similar to convertRowToFields but handles updates specifically.
     */
    private Map<String, Object> convertRowToFieldsForUpdate(Object[] row) {
      Map<String, Object> fields = new LinkedHashMap<>();

      // Skip the ID field (index 0) - we don't update the ID
      List<SharePointColumn> columns = metadata.getColumns();

      for (int i = 0; i < columns.size() && i + 1 < row.length; i++) {
        SharePointColumn column = columns.get(i);
        Object value = row[i + 1]; // +1 to skip ID column

        // For updates, include all fields (even nulls might be intentional)
        fields.put(column.getInternalName(), value);
      }

      return fields;
    }
  }

  private SqlTypeName mapSharePointTypeToSql(String sharePointType) {
    switch (sharePointType.toLowerCase(Locale.ROOT)) {
    case "text":
    case "note":
    case "choice":
    case "multichoice":
    case "lookup":
    case "user":
    case "usergroup":
      return SqlTypeName.VARCHAR;
    case "number":
    case "currency":
      return SqlTypeName.DOUBLE;
    case "integer":
    case "counter":
      return SqlTypeName.INTEGER;
    case "boolean":
      return SqlTypeName.BOOLEAN;
    case "datetime":
      return SqlTypeName.TIMESTAMP;
    case "url":
    case "hyperlink":
      return SqlTypeName.VARCHAR;
    default:
      return SqlTypeName.VARCHAR;
    }
  }

}
