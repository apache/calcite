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
package org.apache.calcite.schema;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface for tables that provide comments for JDBC metadata.
 *
 * <p>This interface allows tables to provide business definitions and comments
 * that are exposed through JDBC metadata operations like 
 * {@link java.sql.DatabaseMetaData#getTables()} and 
 * {@link java.sql.DatabaseMetaData#getColumns()}.
 *
 * <p>Table comments appear in the REMARKS column of getTables() results,
 * while column comments appear in the REMARKS column of getColumns() results.
 *
 * <p>Example usage:
 * <pre>
 * public class MyTable implements Table, CommentableTable {
 *   public String getTableComment() {
 *     return "Employee data with personal and employment information";
 *   }
 *   
 *   public String getColumnComment(String columnName) {
 *     switch (columnName.toLowerCase()) {
 *       case "employee_id": return "Unique identifier for each employee";
 *       case "hire_date": return "Date when employee was hired (YYYY-MM-DD format)";
 *       default: return null;
 *     }
 *   }
 * }
 * </pre>
 */
public interface CommentableTable extends Table {
  
  /**
   * Returns a comment describing this table's business purpose and contents.
   * 
   * <p>This comment is exposed through JDBC metadata as the REMARKS column
   * in {@link java.sql.DatabaseMetaData#getTables()} results.
   * 
   * @return table comment, or null if no comment is available
   */
  @Nullable String getTableComment();
  
  /**
   * Returns a comment describing the specified column's business meaning.
   * 
   * <p>This comment is exposed through JDBC metadata as the REMARKS column
   * in {@link java.sql.DatabaseMetaData#getColumns()} results.
   * 
   * @param columnName name of the column (case-insensitive)
   * @return column comment, or null if no comment is available for this column
   */
  @Nullable String getColumnComment(String columnName);
}