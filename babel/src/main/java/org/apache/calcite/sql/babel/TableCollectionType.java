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
package org.apache.calcite.sql.babel;

/**
 * Enumerates the collection type of a table: {@code MULTISET} allows duplicates
 * and {@code SET} does not.
 *
 * <p>This feature is supported in Teradata, which originally required rows in a
 * table to be unique, and later added the {@code MULTISET} keyword to
 * its {@code CREATE TABLE} command to allow the duplicate rows.
 *
 * <p>In other databases and in the SQL standard, {@code MULTISET} is the only
 * supported option, so there is no explicit syntax.
 */
public enum TableCollectionType {
  /**
   * Table collection type is not specified.
   *
   * <p>Defaults to {@code MULTISET} in ANSI mode,
   * and {@code SET} in Teradata mode.
   */
  UNSPECIFIED,

  /**
   * Duplicate rows are not permitted.
   */
  SET,

  /**
   * Duplicate rows are permitted, in compliance with the ANSI SQL:2011 standard.
   */
  MULTISET,
}
