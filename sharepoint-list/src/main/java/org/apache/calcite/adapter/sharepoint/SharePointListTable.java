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
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Table implementation for SharePoint lists.
 */
public class SharePointListTable extends AbstractTable implements ScannableTable {
  private final SharePointListMetadata metadata;
  private final SharePointRestClient client;

  public SharePointListTable(SharePointListMetadata metadata, SharePointRestClient client) {
    this.metadata = metadata;
    this.client = client;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();

    for (SharePointColumn column : metadata.getColumns()) {
      names.add(column.getName());
      types.add(typeFactory.createSqlType(mapSharePointTypeToSql(column.getType())));
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
