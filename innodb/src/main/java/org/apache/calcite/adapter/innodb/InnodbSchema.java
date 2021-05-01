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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.innodb.java.reader.TableReaderFactory;
import com.alibaba.innodb.java.reader.column.ColumnType;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlFileTableDefProvider;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.stream.Collectors.toList;

/**
 * Schema for an InnoDB data source.
 */
public class InnodbSchema extends AbstractSchema {
  final List<String> sqlFilePathList;
  final String ibdDataFileBasePath;
  final TableReaderFactory tableReaderFactory;

  static final ColumnTypeToSqlTypeConversionRules COLUMN_TYPE_TO_SQL_TYPE =
      ColumnTypeToSqlTypeConversionRules.instance();

  public InnodbSchema(List<String> sqlFilePathList,
      String ibdDataFileBasePath) {
    checkArgument(CollectionUtils.isNotEmpty(sqlFilePathList),
        "SQL file path list cannot be empty");
    checkArgument(StringUtils.isNotEmpty(ibdDataFileBasePath),
        "InnoDB data file with ibd suffix cannot be empty");
    this.sqlFilePathList = sqlFilePathList;
    this.ibdDataFileBasePath = ibdDataFileBasePath;

    List<TableDefProvider> tableDefProviderList = sqlFilePathList.stream()
        .map(SqlFileTableDefProvider::new).collect(toList());
    this.tableReaderFactory = TableReaderFactory.builder()
        .withProviders(tableDefProviderList)
        .withDataFileBasePath(ibdDataFileBasePath)
        .build();
  }

  RelProtoDataType getRelDataType(String tableName) {
    // Temporary type factory, just for the duration of this method. Allowable
    // because we're creating a proto-type, not a type; before being used, the
    // proto-type will be copied into a real type factory.
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    if (!tableReaderFactory.existTableDef(tableName)) {
      throw new RuntimeException("Table definition " + tableName
          + " not found");
    }
    TableDef tableDef = tableReaderFactory.getTableDef(tableName);
    for (Column column : tableDef.getColumnList()) {
      final SqlTypeName sqlTypeName =
          COLUMN_TYPE_TO_SQL_TYPE.lookup(column.getType());
      final int precision;
      final int scale;
      switch (column.getType()) {
      case ColumnType.TIMESTAMP:
      case ColumnType.TIME:
      case ColumnType.DATETIME:
        precision = column.getPrecision();
        scale = 0;
        break;
      default:
        precision = column.getPrecision();
        scale = column.getScale();
        break;
      }
      if (sqlTypeName.allowsPrecScale(true, true)
          && column.getPrecision() >= 0
          && column.getScale() >= 0) {
        fieldInfo.add(column.getName(), sqlTypeName, precision, scale);
      } else if (sqlTypeName.allowsPrecNoScale() && precision >= 0) {
        fieldInfo.add(column.getName(), sqlTypeName, precision);
      } else {
        assert sqlTypeName.allowsNoPrecNoScale();
        fieldInfo.add(column.getName(), sqlTypeName);
      }
      fieldInfo.nullable(column.isNullable());
    }
    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  /**
   * Return table definition.
   */
  public TableDef getTableDef(String tableName) {
    if (!tableReaderFactory.existTableDef(tableName)) {
      throw new RuntimeException("cannot find table definition for " + tableName);
    }
    return tableReaderFactory.getTableDef(tableName);
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    Map<String, TableDef> map = tableReaderFactory.getTableNameToDefMap();
    for (Map.Entry<String, TableDef> entry : map.entrySet()) {
      String tableName = entry.getKey();
      builder.put(tableName, new InnodbTable(this, tableName));
    }
    return builder.build();
  }
}
