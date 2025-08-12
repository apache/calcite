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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL rewriter that transforms Calcite SQL to DuckDB dialect.
 * 
 * <p>Handles:
 * <ul>
 *   <li>Schema and table name transformations</li>
 *   <li>Function name mappings</li>
 *   <li>Data type conversions</li>
 *   <li>Identifier quoting and casing</li>
 * </ul>
 */
public class DuckDBSqlRewriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBSqlRewriter.class);
  
  // Pattern to match table references in FROM clause
  // Matches: FROM table, FROM schema.table, FROM "schema"."table", etc.
  private static final Pattern FROM_TABLE_PATTERN = Pattern.compile(
      "\\bFROM\\s+([\"']?)(\\w+)\\1(?:\\.([\"']?)(\\w+)\\3)?",
      Pattern.CASE_INSENSITIVE
  );
  
  // Pattern to match JOIN table references
  private static final Pattern JOIN_TABLE_PATTERN = Pattern.compile(
      "\\bJOIN\\s+([\"']?)(\\w+)\\1(?:\\.([\"']?)(\\w+)\\3)?",
      Pattern.CASE_INSENSITIVE
  );
  
  /**
   * Rewrites Calcite SQL to DuckDB dialect.
   * 
   * @param sql The Calcite SQL query
   * @param defaultSchema The default schema name if not qualified
   * @return The rewritten SQL for DuckDB
   */
  public static String rewrite(String sql, String defaultSchema) {
    if (sql == null || sql.trim().isEmpty()) {
      return sql;
    }
    
    LOGGER.debug("Rewriting SQL for DuckDB. Original: {}", sql);
    
    String rewritten = sql;
    
    // Step 1: Handle table references
    rewritten = rewriteTableReferences(rewritten, defaultSchema);
    
    // Step 2: Handle function mappings
    rewritten = rewriteFunctions(rewritten);
    
    // Step 3: Handle data type conversions
    rewritten = rewriteDataTypes(rewritten);
    
    // Step 4: Handle identifier quoting
    rewritten = rewriteIdentifiers(rewritten);
    
    LOGGER.debug("Rewritten SQL for DuckDB: {}", rewritten);
    return rewritten;
  }
  
  /**
   * Rewrites table references to use proper schema qualification.
   */
  private static String rewriteTableReferences(String sql, String defaultSchema) {
    if (defaultSchema == null) {
      return sql;
    }
    
    // Handle FROM clause tables
    StringBuffer result = new StringBuffer();
    Matcher fromMatcher = FROM_TABLE_PATTERN.matcher(sql);
    
    while (fromMatcher.find()) {
      String quote1 = fromMatcher.group(1);
      String schemaOrTable = fromMatcher.group(2);
      String quote2 = fromMatcher.group(3);
      String table = fromMatcher.group(4);
      
      String replacement;
      if (table != null) {
        // Already schema-qualified
        replacement = "FROM " + quote1 + schemaOrTable + quote1 + "." + quote2 + table + quote2;
      } else {
        // Add default schema
        replacement = "FROM " + defaultSchema + "." + quote1 + schemaOrTable + quote1;
      }
      
      fromMatcher.appendReplacement(result, replacement);
    }
    fromMatcher.appendTail(result);
    
    // Handle JOIN clause tables
    String intermediate = result.toString();
    result = new StringBuffer();
    Matcher joinMatcher = JOIN_TABLE_PATTERN.matcher(intermediate);
    
    while (joinMatcher.find()) {
      String quote1 = joinMatcher.group(1);
      String schemaOrTable = joinMatcher.group(2);
      String quote2 = joinMatcher.group(3);
      String table = joinMatcher.group(4);
      
      String replacement;
      if (table != null) {
        // Already schema-qualified
        replacement = "JOIN " + quote1 + schemaOrTable + quote1 + "." + quote2 + table + quote2;
      } else {
        // Add default schema
        replacement = "JOIN " + defaultSchema + "." + quote1 + schemaOrTable + quote1;
      }
      
      joinMatcher.appendReplacement(result, replacement);
    }
    joinMatcher.appendTail(result);
    
    return result.toString();
  }
  
  /**
   * Rewrites function names from Calcite to DuckDB dialect.
   */
  private static String rewriteFunctions(String sql) {
    // Map Calcite functions to DuckDB equivalents
    String result = sql;
    
    // STDDEV -> STDDEV_SAMP (DuckDB uses more specific names)
    result = result.replaceAll("\\bSTDDEV\\s*\\(", "STDDEV_SAMP(");
    
    // SUBSTRING -> SUBSTR (both work in DuckDB, but SUBSTR is preferred)
    result = result.replaceAll("\\bSUBSTRING\\s*\\(", "SUBSTR(");
    
    // CHAR_LENGTH -> LENGTH (DuckDB uses LENGTH for character count)
    result = result.replaceAll("\\bCHAR_LENGTH\\s*\\(", "LENGTH(");
    
    // Add more function mappings as needed
    
    return result;
  }
  
  /**
   * Rewrites data type conversions.
   */
  private static String rewriteDataTypes(String sql) {
    // Map Calcite data types to DuckDB equivalents
    String result = sql;
    
    // CAST(x AS INTEGER) -> CAST(x AS INT)
    result = result.replaceAll("\\bINTEGER\\b", "INT");
    
    // CAST(x AS FLOAT) -> CAST(x AS REAL)  
    result = result.replaceAll("\\bFLOAT\\b", "REAL");
    
    // Add more data type mappings as needed
    
    return result;
  }
  
  /**
   * Rewrites identifier quoting and casing.
   */
  private static String rewriteIdentifiers(String sql) {
    // DuckDB prefers double quotes for identifiers
    // Convert backticks to double quotes
    String result = sql.replaceAll("`([^`]+)`", "\"$1\"");
    
    // Remove unnecessary quotes from simple identifiers
    // DuckDB doesn't need quotes for simple lowercase identifiers
    result = result.replaceAll("\"([a-z_][a-z0-9_]*)\"", "$1");
    
    return result;
  }
  
  /**
   * Escapes a string value for use in DuckDB SQL.
   */
  public static String escapeString(String value) {
    if (value == null) {
      return "NULL";
    }
    // Escape single quotes by doubling them
    return "'" + value.replace("'", "''") + "'";
  }
  
  /**
   * Quotes an identifier if necessary for DuckDB.
   */
  public static String quoteIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    
    // Check if identifier needs quoting
    // DuckDB requires quotes for:
    // - Mixed case identifiers
    // - Identifiers with special characters
    // - Reserved keywords
    
    if (identifier.matches("^[a-z_][a-z0-9_]*$")) {
      // Simple lowercase identifier doesn't need quotes
      return identifier;
    }
    
    // Quote the identifier
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}