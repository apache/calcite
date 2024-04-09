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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

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
  STANDARD("", "standard"),
  /** Geospatial operators. */
  SPATIAL("s", "spatial"),
  /** A collection of operators that could be used in all libraries;
   * does not include STANDARD and SPATIAL. */
  ALL("*", "all"),
  /** A collection of operators that are in Google BigQuery but not in standard
   * SQL. */
  BIG_QUERY("b", "bigquery"),
  /** Calcite-specific extensions. */
  CALCITE("c", "calcite"),
  /** A collection of operators that are in Apache Hive but not in standard
   * SQL. */
  HIVE("h", "hive"),
  /** A collection of operators that are in Microsoft SQL Server (MSSql) but not
   * in standard SQL. */
  MSSQL("q", "mssql"),
  /** A collection of operators that are in MySQL but not in standard SQL. */
  MYSQL("m", "mysql"),
  /** A collection of operators that are in Oracle but not in standard SQL. */
  ORACLE("o", "oracle"),
  /** A collection of operators that are in PostgreSQL but not in standard
   * SQL. */
  POSTGRESQL("p", "postgresql"),
  /** A collection of operators that are in Snowflake but not in standard SQL. */
  SNOWFLAKE("f", "snowflake"),
  /** A collection of operators that are in Apache Spark but not in standard
   * SQL. */
  SPARK("s", "spark");

  /** Map from {@link Enum#name() name} and {@link #fun} to library. */
  public static final Map<String, SqlLibrary> MAP;

  /** Abbreviation for the library used in SQL reference. */
  public final String abbrev;

  /** Name of this library when it appears in the connect string;
   * see {@link CalciteConnectionProperty#FUN}. */
  public final String fun;

  SqlLibrary(String abbrev, String fun) {
    this.abbrev = requireNonNull(abbrev, "abbrev");
    this.fun = requireNonNull(fun, "fun");
    checkArgument(fun.equals(name().toLowerCase(Locale.ROOT).replace("_", "")));
  }

  @SuppressWarnings("SwitchStatementWithTooFewBranches")
  public List<SqlLibrary> children() {
    switch (this) {
    case ALL:
      return ImmutableList.of(BIG_QUERY, CALCITE, HIVE, MSSQL, MYSQL, ORACLE,
          POSTGRESQL, SNOWFLAKE, SPARK);
    default:
      return ImmutableList.of();
    }
  }

  /** Looks up a value.
   * Returns null if not found.
   * You can use upper- or lower-case name. */
  public static @Nullable SqlLibrary of(String name) {
    return MAP.get(name);
  }

  /** Parses a comma-separated string such as "standard,oracle". */
  public static List<SqlLibrary> parse(String libraryNameList) {
    final ImmutableList.Builder<SqlLibrary> list = ImmutableList.builder();
    if (!libraryNameList.isEmpty()) {
      for (String libraryName : libraryNameList.split(",")) {
        @Nullable SqlLibrary library = SqlLibrary.of(libraryName);
        if (library == null) {
          throw new IllegalArgumentException("unknown library '" + libraryName
              + "'");
        }
        list.add(library);
      }
    }
    return list.build();
  }

  /** Expands libraries in place.
   *
   * <p>Preserves order, and ensures that no library occurs more than once. */
  public static List<SqlLibrary> expand(
      Iterable<? extends SqlLibrary> libraries) {
    // LinkedHashSet ensures that libraries are added only once, and order is
    // preserved.
    final Set<SqlLibrary> set = new LinkedHashSet<>();
    libraries.forEach(library -> addExpansion(set, library));
    return ImmutableList.copyOf(set);
  }

  private static void addExpansion(Set<SqlLibrary> set, SqlLibrary library) {
    if (set.add(library)) {
      library.children().forEach(subLibrary -> addExpansion(set, subLibrary));
    }
  }

  /** Expands libraries in place. If any library is a child of 'all', ensures
   * that 'all' is in the list. */
  public static List<SqlLibrary> expandUp(
      Iterable<? extends SqlLibrary> libraries) {
    // LinkedHashSet ensures that libraries are added only once, and order is
    // preserved.
    final Set<SqlLibrary> set = new LinkedHashSet<>();
    libraries.forEach(library -> addParent(set, library));
    return ImmutableList.copyOf(set);
  }

  private static void addParent(Set<SqlLibrary> set, SqlLibrary library) {
    if (ALL.children().contains(library)) {
      set.add(ALL);
    }
    set.add(library);
  }

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
