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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Table function that executes the OS "vmstat" command
 * to share memory statistics.
 */
public class VmstatTableFunction {

  private VmstatTableFunction() {}

  public static ScannableTable eval(boolean b) {
    return new ScannableTable() {
      public Enumerable<Object[]> scan(DataContext root) {
        final RelDataType rowType = getRowType(root.getTypeFactory());
        final List<String> fieldNames =
            ImmutableList.copyOf(rowType.getFieldNames());
        final String[] args;
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        Util.discard(osVersion);
        // Fork out to a shell so that we can get normal text-munging support.
        // Could do this here too..
        switch (osName) {
        case "Mac OS X": // tested on version 10.11.6
          args = new String[] {
              "/bin/sh", "-c",
              "vm_stat | tail -n +2 | awk '{print $NF}' | sed 's/\\.//' | tr '\\n' ' '"
          };
          break;
        default:
          args = new String[]{"/bin/sh", "-c", "vmstat -n | tail -n +3"};
        }
        return Processes.processLines(args)
            .select(
                new Function1<String, Object[]>() {
                  public Object[] apply(String line) {
                    final String[] fields = line.trim().split("\\s+");
                    final Object[] values = new Object[fieldNames.size()];
                    for (int i = 0; i < values.length; i++) {
                      try {
                        values[i] = field(fieldNames.get(i), fields[i]);
                      } catch (RuntimeException e) {
                        e.printStackTrace(System.out);
                        throw new RuntimeException("while parsing value ["
                            + fields[i] + "] of field [" + fieldNames.get(i)
                            + "] in line [" + line + "]");
                      }
                    }
                    return values;
                  }

                  private Object field(String field, String value) {
                    if (value.isEmpty()) {
                      return 0;
                    }
                    if (value.endsWith(".")) {
                      return Long.parseLong(value);
                    }
                    return Long.parseLong(value);
                  }
                });
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final String osName = System.getProperty("os.name");
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        switch (osName) {
        case "Mac OS X":
          return builder
              .add("pages_free", SqlTypeName.BIGINT)
              .add("pages_active", SqlTypeName.BIGINT)
              .add("pages_inactive", SqlTypeName.BIGINT)
              .add("pages_speculative", SqlTypeName.BIGINT)
              .add("pages_throttled", SqlTypeName.BIGINT)
              .add("pages_wired_down", SqlTypeName.BIGINT)
              .add("pages_purgeable", SqlTypeName.BIGINT)
              .add("translation_faults", SqlTypeName.BIGINT)
              .add("pages_copy_on_write", SqlTypeName.BIGINT)
              .add("pages_zero_filed", SqlTypeName.BIGINT)
              .add("pages_reactivated", SqlTypeName.BIGINT)
              .add("pages_purged", SqlTypeName.BIGINT)
              .add("pages_file_backed", SqlTypeName.BIGINT)
              .add("pages_anonymous", SqlTypeName.BIGINT)
              .add("pages_stored_compressor", SqlTypeName.BIGINT)
              .add("pages_occupied_compressor", SqlTypeName.BIGINT)
              .add("decompressions", SqlTypeName.BIGINT)
              .add("compressions", SqlTypeName.BIGINT)
              .add("pageins", SqlTypeName.BIGINT)
              .add("pageouts", SqlTypeName.BIGINT)
              .add("swapins", SqlTypeName.BIGINT)
              .add("swapouts", SqlTypeName.BIGINT)
              .build();
        default:
          return builder
              .add("proc_r", SqlTypeName.BIGINT)
              .add("proc_b", SqlTypeName.BIGINT)
              .add("mem_swpd", SqlTypeName.BIGINT)
              .add("mem_free", SqlTypeName.BIGINT)
              .add("mem_buff", SqlTypeName.BIGINT)
              .add("mem_cache", SqlTypeName.BIGINT)
              .add("swap_si", SqlTypeName.BIGINT)
              .add("swap_so", SqlTypeName.BIGINT)
              .add("io_bi", SqlTypeName.BIGINT)
              .add("io_bo", SqlTypeName.BIGINT)
              .add("system_in", SqlTypeName.BIGINT)
              .add("system_cs", SqlTypeName.BIGINT)
              .add("cpu_us", SqlTypeName.BIGINT)
              .add("cpu_sy", SqlTypeName.BIGINT)
              .add("cpu_id", SqlTypeName.BIGINT)
              .add("cpu_wa", SqlTypeName.BIGINT)
              .add("cpu_st", SqlTypeName.BIGINT)
              .build();
        }
      }

      public Statistic getStatistic() {
        return Statistics.of(1000d, ImmutableList.of(ImmutableBitSet.of(1)));
      }

      public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
      }

      public boolean isRolledUp(String column) {
        return false;
      }

      public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          SqlNode parent, CalciteConnectionConfig config) {
        return true;
      }
    };
  }
}

// End VmstatTableFunction.java
