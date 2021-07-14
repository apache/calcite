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
package org.apache.calcite.adapter.os;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Table function that executes the OS "system_info".
 */
public class SystemInfoTableFunction {
  private SystemInfoTableFunction() {
  }

  public static ScannableTable eval(boolean b) {
    return new AbstractBaseScannableTable() {
      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            return new OsQuery("system_info");
          }
        };
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("hostname", SqlTypeName.VARCHAR)
            .add("uuid", SqlTypeName.VARCHAR)
            .add("cpu_type", SqlTypeName.VARCHAR)
            .add("cpu_vendor", SqlTypeName.VARCHAR)
            .add("cpu_model", SqlTypeName.VARCHAR)
            .add("cpu_physical_cores", SqlTypeName.INTEGER)
            .add("cpu_logical_cores", SqlTypeName.INTEGER)
            .add("cpu_sockets", SqlTypeName.INTEGER)
            .add("physical_memory", SqlTypeName.BIGINT)
            .add("hardware_vendor", SqlTypeName.VARCHAR)
            .add("hardware_model", SqlTypeName.VARCHAR)
            .add("hardware_version", SqlTypeName.VARCHAR)
            .add("hardware_serial", SqlTypeName.VARCHAR)
            .add("board_vendor", SqlTypeName.VARCHAR)
            .add("board_model", SqlTypeName.VARCHAR)
            .add("board_version", SqlTypeName.VARCHAR)
            .add("board_serial", SqlTypeName.VARCHAR)
            .add("computer_name", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }
}
