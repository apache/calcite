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
package org.apache.calcite.sql.fun;

import org.apache.calcite.config.CalciteConnectionProperty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A library is a collection of SQL functions and operators.
 *
 * <p>Typically, such collections are associated with a particular dialect or
 * database. For example, {@link SqlLibrary#ORACLE} is a collection of functions
 * that are in the Oracle database but not the SQL standard.
 *
 * <p>In {@link SqlLibraryOperatorTableFactory} this annotation is applied to
 * function definitions to include them in a particular library. It allows
 * an operator to belong to more than one library.
 *
 * @see LibraryOperator
 */
public enum SqlLibrary {
  /** The standard operators. */
  STANDARD(""),
  /** Geospatial operators. */
  SPATIAL("s"),
  /** A collection of operators that are in MySQL but not in standard SQL. */
  MYSQL("m"),
  /** A collection of operators that are in Oracle but not in standard SQL. */
  ORACLE("o"),
  /** A collection of operators that are in PostgreSQL but not in standard
   * SQL. */
  POSTGRESQL("p");

  /** Abbreviation for the library used in SQL reference. */
  public final String abbrev;

  /** Name of this library when it appears in the connect string;
   * see {@link CalciteConnectionProperty#FUN}. */
  public final String fun;

  SqlLibrary(String abbrev) {
    this.abbrev = Objects.requireNonNull(abbrev);
    this.fun = name().toLowerCase(Locale.ROOT);
  }

  /** Looks up a value.
   * Returns null if not found.
   * You can use upper- or lower-case name. */
  public static SqlLibrary of(String name) {
    return MAP.get(name);
  }

  /** Parses a comma-separated string such as "standard,oracle". */
  public static List<SqlLibrary> parse(String libraryNameList) {
    final ImmutableList.Builder<SqlLibrary> list = ImmutableList.builder();
    for (String libraryName : libraryNameList.split(",")) {
      list.add(SqlLibrary.of(libraryName));
    }
    return list.build();
  }

  public static final Map<String, SqlLibrary> MAP;

  static {
    final ImmutableMap.Builder<String, SqlLibrary> builder =
        ImmutableMap.builder();
    for (SqlLibrary value : values()) {
      builder.put(value.name(), value);
      builder.put(value.fun, value);
    }
    MAP = builder.build();
  }
}

// End SqlLibrary.java
