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
import org.apache.calcite.avatica.util.TimeUnit;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Table function that executes the OS "ps" command
 * to list processes.
 */
public class PsTableFunction {
  private static final Pattern MINUTE_SECOND_MILLIS_PATTERN =
      Pattern.compile("([0-9]+):([0-9]+):([0-9]+)");
  private static final Pattern HOUR_MINUTE_SECOND_PATTERN =
      Pattern.compile("([0-9]+):([0-9]+)\\.([0-9]+)");

  private PsTableFunction() {}

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
        switch (osName) {
        case "Mac OS X": // tested on version 10.12.5
          args = new String[] {
              "ps", "ax", "-o", "ppid=,pid=,pgid=,tpgid=,stat=,"
                + "user=,pcpu=,pmem=,vsz=,rss=,tty=,start=,time=,uid=,ruid=,"
                + "sess=,comm="};
          break;
        default:
          args = new String[] {
              "ps", "--no-headers", "axo", "ppid,pid,pgrp,"
                + "tpgid,stat,user,pcpu,pmem,vsz,rss,tty,start_time,time,euid,"
                + "ruid,sess,comm"};
        }
        return Processes.processLines(args)
            .select(
                new Function1<String, Object[]>() {
                  public Object[] apply(String line) {
                    final String[] fields = line.trim().split(" +");
                    final Object[] values = new Object[fieldNames.size()];
                    for (int i = 0; i < values.length; i++) {
                      try {
                        values[i] = field(fieldNames.get(i), fields[i]);
                      } catch (RuntimeException e) {
                        throw new RuntimeException("while parsing value ["
                            + fields[i] + "] of field [" + fieldNames.get(i)
                            + "] in line [" + line + "]");
                      }
                    }
                    return values;
                  }

                  private Object field(String field, String value) {
                    switch (field) {
                    case "pid":
                    case "ppid":
                    case "pgrp": // linux only; macOS equivalent is "pgid"
                    case "pgid": // see "pgrp"
                    case "tpgid":
                      return Integer.valueOf(value);
                    case "pcpu":
                    case "pmem":
                      return (int) (Float.valueOf(value) * 10f);
                    case "time":
                      final Matcher m1 =
                          MINUTE_SECOND_MILLIS_PATTERN.matcher(value);
                      if (m1.matches()) {
                        final long h = Long.parseLong(m1.group(1));
                        final long m = Long.parseLong(m1.group(2));
                        final long s = Long.parseLong(m1.group(3));
                        return h * 3600000L + m * 60000L + s * 1000L;
                      }
                      final Matcher m2 =
                          HOUR_MINUTE_SECOND_PATTERN.matcher(value);
                      if (m2.matches()) {
                        final long m = Long.parseLong(m2.group(1));
                        final long s = Long.parseLong(m2.group(2));
                        String g3 = m2.group(3);
                        while (g3.length() < 3) {
                          g3 = g3 + "0";
                        }
                        final long millis = Long.parseLong(g3);
                        return m * 60000L + s * 1000L + millis;
                      }
                      return 0L;
                    case "start_time": // linux only; macOS version is "lstart"
                    case "lstart": // see "start_time"
                    case "euid": // linux only; macOS equivalent is "uid"
                    case "uid": // see "euid"
                    default:
                      return value;
                    }
                  }
                });
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("pid", SqlTypeName.INTEGER)
            .add("ppid", SqlTypeName.INTEGER)
            .add("pgrp", SqlTypeName.INTEGER)
            .add("tpgid", SqlTypeName.INTEGER)
            .add("stat", SqlTypeName.VARCHAR)
            .add("user", SqlTypeName.VARCHAR)
            .add("pcpu", SqlTypeName.DECIMAL, 3, 1)
            .add("pmem", SqlTypeName.DECIMAL, 3, 1)
            .add("vsz", SqlTypeName.INTEGER)
            .add("rss", SqlTypeName.INTEGER)
            .add("tty", SqlTypeName.VARCHAR)
            .add("start_time", SqlTypeName.VARCHAR)
            .add("time", TimeUnit.HOUR, -1, TimeUnit.SECOND, 0)
            .add("euid", SqlTypeName.VARCHAR)
            .add("ruid", SqlTypeName.VARCHAR)
            .add("sess", SqlTypeName.VARCHAR)
            .add("command", SqlTypeName.VARCHAR)
            .build();
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

// End PsTableFunction.java
