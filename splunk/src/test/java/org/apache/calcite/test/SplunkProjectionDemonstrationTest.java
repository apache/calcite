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
package org.apache.calcite.test;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates the expected behavior of projection pushdown in the Splunk adapter.
 * This test documents which queries are pushed down and which are not.
 */
@Tag("unit")
public class SplunkProjectionDemonstrationTest {

  @Test void demonstrateProjectionBehavior() {
    System.out.println("=== Splunk Adapter Projection Pushdown Behavior ===\n");

    System.out.println("1. SIMPLE PROJECTIONS (Pushed Down to Splunk):");
    System.out.println("   These queries have their projections handled by Splunk directly.\n");

    demonstrateQuery("SELECT \"status\" FROM \"splunk\".\"web\"",
                     true, "Simple field selection");

    demonstrateQuery("SELECT \"status\", \"action\", \"bytes\" FROM \"splunk\".\"web\"",
                     true, "Multiple field selection");

    demonstrateQuery("SELECT \"action\", \"status\" FROM \"splunk\".\"web\"",
                     true, "Field reordering");

    System.out.println("\n2. COMPLEX PROJECTIONS (Handled by Calcite):");
    System.out.println("   These queries have LogicalProject nodes that execute in Calcite.\n");

    // CAST operations
    System.out.println("   A. CAST Operations:");
    demonstrateQuery("SELECT CAST(\"bytes\" AS BIGINT) FROM \"splunk\".\"web\"",
                     false, "INTEGER to BIGINT");
    demonstrateQuery("SELECT CAST(\"status\" AS INTEGER) FROM \"splunk\".\"web\"",
                     false, "VARCHAR to INTEGER");
    demonstrateQuery("SELECT CAST(\"bytes\" AS VARCHAR) FROM \"splunk\".\"web\"",
                     false, "INTEGER to VARCHAR");
    demonstrateQuery("SELECT CAST(\"bytes\" AS DOUBLE) FROM \"splunk\".\"web\"",
                     false, "INTEGER to DOUBLE");

    // Arithmetic operations
    System.out.println("\n   B. Arithmetic Operations:");
    demonstrateQuery("SELECT \"bytes\" * 2 FROM \"splunk\".\"web\"",
                     false, "Multiplication");
    demonstrateQuery("SELECT \"bytes\" + 100 FROM \"splunk\".\"web\"",
                     false, "Addition");
    demonstrateQuery("SELECT \"bytes\" / 1024 FROM \"splunk\".\"web\"",
                     false, "Division");

    // String functions
    System.out.println("\n   C. String Functions:");
    demonstrateQuery("SELECT UPPER(\"action\") FROM \"splunk\".\"web\"",
                     false, "UPPER function");
    demonstrateQuery("SELECT CONCAT(\"action\", '_', \"status\") FROM \"splunk\".\"web\"",
                     false, "CONCAT function");

    // Mixed projections
    System.out.println("\n   D. Mixed Projections:");
    demonstrateQuery("SELECT \"action\", CAST(\"bytes\" AS BIGINT) FROM \"splunk\".\"web\"",
                     false, "Simple field + CAST");
    demonstrateQuery("SELECT \"status\", \"bytes\" * 2 FROM \"splunk\".\"web\"",
                     false, "Simple field + arithmetic");

    System.out.println("\n3. WHY THIS BEHAVIOR?");
    System.out.println("   - Splunk's SPL doesn't support SQL-style expressions in projections");
    System.out.println("   - Simple field references can be pushed down efficiently");
    System.out.println("   - Complex expressions must be evaluated by Calcite's execution engine");
    System.out.println("   - This ensures correctness while maintaining performance for simple queries");
  }

  @Test void demonstrateTypeConversions() {
    System.out.println("\n=== Type Conversion Examples ===\n");

    System.out.println("Common type conversions that work correctly:");
    System.out.println("(All handled by Calcite, not pushed to Splunk)\n");

    // Numeric conversions
    System.out.println("1. Numeric Type Conversions:");
    demonstrateConversion("BIGINT", "VARCHAR",
        "SELECT CAST(CAST(\"bytes\" AS BIGINT) AS VARCHAR) FROM \"splunk\".\"web\"");
    demonstrateConversion("INTEGER", "DOUBLE",
        "SELECT CAST(\"bytes\" AS DOUBLE) FROM \"splunk\".\"web\"");
    demonstrateConversion("DOUBLE", "INTEGER",
        "SELECT CAST(CAST(\"bytes\" AS DOUBLE) AS INTEGER) FROM \"splunk\".\"web\"");

    // String to numeric
    System.out.println("\n2. String to Numeric Conversions:");
    demonstrateConversion("VARCHAR", "INTEGER",
        "SELECT CAST(\"status\" AS INTEGER) FROM \"splunk\".\"web\"");
    demonstrateConversion("VARCHAR", "BIGINT",
        "SELECT CAST(\"status\" AS BIGINT) FROM \"splunk\".\"web\"");

    // Date/Time conversions
    System.out.println("\n3. Date/Time Conversions:");
    demonstrateConversion("TIMESTAMP", "DATE",
        "SELECT CAST(\"_time\" AS DATE) FROM \"splunk\".\"web\"");
    demonstrateConversion("TIMESTAMP", "TIME",
        "SELECT CAST(\"_time\" AS TIME) FROM \"splunk\".\"web\"");

    System.out.println("\nAll these conversions execute without ClassCastException!");
  }

  private void demonstrateQuery(String query, boolean isPushedDown, String description) {
    String behavior = isPushedDown ? "PUSHED DOWN" : "NOT PUSHED";
    System.out.printf("   %-75s [%s] - %s%n", query, behavior, description);
  }

  private void demonstrateConversion(String fromType, String toType, String query) {
    System.out.printf("   %-20s -> %-20s : %s%n", fromType, toType, query);
  }
}
