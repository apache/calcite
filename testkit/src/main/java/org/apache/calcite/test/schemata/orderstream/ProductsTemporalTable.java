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
package org.apache.calcite.test.schemata.orderstream;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table representing the PRODUCTS_TEMPORAL temporal table.
 */
public class ProductsTemporalTable implements TemporalTable {

  private final RelProtoDataType protoRowType = a0 -> a0.builder()
      .add("ID", SqlTypeName.VARCHAR, 32)
      .add("SUPPLIER", SqlTypeName.INTEGER)
      .add("SYS_START", SqlTypeName.TIMESTAMP)
      .add("SYS_END", SqlTypeName.TIMESTAMP)
      .build();

  @Override public String getSysStartFieldName() {
    return "SYS_START";
  }

  @Override public String getSysEndFieldName() {
    return "SYS_END";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  @Override public Statistic getStatistic() {
    return Statistics.of(200d, ImmutableList.of());
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override public boolean isRolledUp(String column) {
    return false;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
    return false;
  }
}
