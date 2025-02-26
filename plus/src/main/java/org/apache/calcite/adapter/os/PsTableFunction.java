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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;

/**
 * Table function that executes the OS "ps" command
 * to list processes.
 */
public class PsTableFunction {
  private static final Pattern MINUTE_SECOND_MILLIS_PATTERN =
      Pattern.compile("([0-9]+):([0-9]+):([0-9]+)");
  private static final Pattern HOUR_MINUTE_SECOND_PATTERN =
      Pattern.compile("([0-9]+):([0-9]+)\\.([0-9]+)");
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("(\\d+)");

  // it acts as a partial mapping, missing entries are the identity (e.g., "user" -> "user")
  private static final ImmutableMap<String, String> UNIX_TO_MAC_PS_FIELDS =
      ImmutableMap.<String, String>builder()
          .put("pgrp", "pgid")
          .put("start_time", "lstart")
          .put("euid", "uid")
          .build();

  private static final List<String> PS_FIELD_NAMES =
      ImmutableList.of("user",
      "pid",
      "ppid",
      "pgrp",
      "tpgid",
      "stat",
      "pcpu",
      "pmem",
      "vsz",
      "rss",
      "tty",
      "start_time",
      "time",
      "euid",
      "ruid",
      "sess",
      "comm");

  private PsTableFunction() {
    throw new AssertionError("Utility class should not be instantiated");
  }

  /**
   * Class for parsing, line by line, the output of the ps command for a
   * predefined list of parameters.
   */
  @VisibleForTesting
  protected static class LineParser implements Function1<String, Object[]> {

    @Override public Object[] apply(String line) {
      final String[] tokens = line.trim().split(" +");
      final Object[] values = new Object[PS_FIELD_NAMES.size()];

      if (tokens.length < PS_FIELD_NAMES.size()) {
        throw new IllegalArgumentException(
            "Expected at least " + PS_FIELD_NAMES.size() + ", got " + tokens.length);
      }

      int fieldIdx = 0;
      int processedTokens = 0;
      // more tokens than fields, either "user" or "comm" (or both) contain whitespaces, we assume
      // usernames don't have numeric parts separated by whitespaces (e.g., "root 123"), therefore
      // we stop whenever we find a numeric token assuming it's the "pid" and "user" is over
      if (tokens.length > PS_FIELD_NAMES.size()) {
        StringBuilder sb = new StringBuilder();
        for (String field : tokens) {
          if (NUMERIC_PATTERN.matcher(field).matches()) {
            break;
          }
          processedTokens++;
          sb.append(field).append(" ");
        }
        values[fieldIdx] =
            field(PS_FIELD_NAMES.get(fieldIdx), sb.deleteCharAt(sb.length() - 1).toString());
        fieldIdx++;
      }

      for (; fieldIdx < values.length - 1; fieldIdx++) {
        try {
          values[fieldIdx] = field(PS_FIELD_NAMES.get(fieldIdx), tokens[processedTokens++]);
        } catch (RuntimeException e) {
          throw new RuntimeException("while parsing value ["
              + tokens[fieldIdx] + "] of field [" + PS_FIELD_NAMES.get(fieldIdx)
              + "] in line [" + line + "]");
        }
      }

      // spaces also in the "comm" part
      if (processedTokens < tokens.length - 1) {
        StringBuilder sb = new StringBuilder();
        while (processedTokens < tokens.length) {
          sb.append(tokens[processedTokens++]).append(" ");
        }
        values[fieldIdx] =
            field(PS_FIELD_NAMES.get(fieldIdx), sb.deleteCharAt(sb.length() - 1).toString());
      } else {
        values[fieldIdx] = field(PS_FIELD_NAMES.get(fieldIdx), tokens[processedTokens]);
      }
      return values;
    }

    private static Object field(String field, String value) {
      switch (field) {
      case "pid":
      case "ppid":
      case "pgrp": // linux only; macOS equivalent is "pgid"
      case "pgid": // see "pgrp"
      case "tpgid":
        return Integer.valueOf(value);
      case "pcpu":
      case "pmem":
        return (int) (parseFloat(value) * 10f);
      case "time":
        final Matcher m1 =
            MINUTE_SECOND_MILLIS_PATTERN.matcher(value);
        if (m1.matches()) {
          final long h = parseLong(m1.group(1));
          final long m = parseLong(m1.group(2));
          final long s = parseLong(m1.group(3));
          return h * 3600000L + m * 60000L + s * 1000L;
        }
        final Matcher m2 =
            HOUR_MINUTE_SECOND_PATTERN.matcher(value);
        if (m2.matches()) {
          final long m = parseLong(m2.group(1));
          final long s = parseLong(m2.group(2));
          StringBuilder g3 = new StringBuilder(m2.group(3));
          while (g3.length() < 3) {
            g3.append("0");
          }
          final long millis = parseLong(g3.toString());
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
  }

  public static ScannableTable eval(boolean b) {
    return new AbstractBaseScannableTable() {
      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        final RelDataType rowType = getRowType(root.getTypeFactory());
        final List<String> fieldNames = ImmutableList.copyOf(rowType.getFieldNames());
        final String[] args;
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        Util.discard(osVersion);
        switch (osName) {
        case "Mac OS X": // tested on version 10.12.5
          args = new String[] {
              "ps", "ax", "-o",
              fieldNames.stream()
                  .map(s -> UNIX_TO_MAC_PS_FIELDS.getOrDefault(s, s) + "=")
                  .collect(Collectors.joining(","))};
          break;
        default:
          args = new String[] {
              "ps", "--no-headers", "axo", String.join(",", fieldNames)};
        }
        return Processes.processLines(args).select(new LineParser());
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add(PS_FIELD_NAMES.get(0), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(1), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(2), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(3), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(4), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(5), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(6), SqlTypeName.DECIMAL, 3, 1)
            .add(PS_FIELD_NAMES.get(7), SqlTypeName.DECIMAL, 3, 1)
            .add(PS_FIELD_NAMES.get(8), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(9), SqlTypeName.INTEGER)
            .add(PS_FIELD_NAMES.get(10), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(11), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(12), TimeUnit.HOUR, -1, TimeUnit.SECOND, 0)
            .add(PS_FIELD_NAMES.get(13), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(14), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(15), SqlTypeName.VARCHAR)
            .add(PS_FIELD_NAMES.get(16), SqlTypeName.VARCHAR)
            .build();
      }
    };
  }
}
