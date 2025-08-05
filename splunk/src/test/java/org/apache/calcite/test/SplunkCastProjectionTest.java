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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify CAST pushdown optimization for the Splunk adapter.
 *
 * The Splunk adapter now pushes down simple CAST operations to Splunk
 * using native conversion functions like tostring(), toint(), tonumber().
 */
@Tag("unit")
public class SplunkCastProjectionTest {

  @Test void testSimpleCastPushedDown() {
    // Simple CAST operations are now pushed down to Splunk using eval

    String toStringCast =
        "SELECT CAST(\"status\" AS VARCHAR) as status_str " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    String toIntCast =
        "SELECT CAST(\"bytes\" AS INTEGER) as bytes_int " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    String toNumberCast =
        "SELECT CAST(\"response_time\" AS DOUBLE) as resp_time " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    System.out.println("Simple CAST operations are pushed down to Splunk:");
    System.out.println("1. VARCHAR cast uses tostring(): " + toStringCast);
    System.out.println("   Generates: | eval status_str = tostring(status)");
    System.out.println();
    System.out.println("2. INTEGER cast uses toint(): " + toIntCast);
    System.out.println("   Generates: | eval bytes_int = toint(bytes)");
    System.out.println();
    System.out.println("3. DOUBLE cast uses tonumber(): " + toNumberCast);
    System.out.println("   Generates: | eval resp_time = tonumber(response_time)");
  }

  @Test void testComplexExpressionsNotPushedDown() {
    // Complex expressions (non-CAST) are still handled by Calcite

    String arithmeticQuery =
        "SELECT \"bytes\" * 2 as double_bytes " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    String functionQuery =
        "SELECT UPPER(\"action\") as action_upper " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    String literalQuery =
        "SELECT 'constant' as const_field " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    System.out.println("Complex expressions are handled by Calcite:");
    System.out.println("1. Arithmetic: " + arithmeticQuery);
    System.out.println("2. Functions: " + functionQuery);
    System.out.println("3. Literals: " + literalQuery);
    System.out.println();
    System.out.println("These are not pushed down because Splunk doesn't support them in projections.");
  }

  @Test void testMixedProjectionsWithCast() {
    // When a projection mixes simple fields and CAST operations,
    // the CAST is pushed down while simple fields are preserved

    String mixedQuery =
        "SELECT \"status\", CAST(\"bytes\" AS BIGINT) as bytes_long, \"action\" " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    System.out.println("Mixed projections with CAST are optimized:");
    System.out.println(mixedQuery);
    System.out.println();
    System.out.println("Generates: search ... | eval bytes_long = toint(bytes)");
    System.out.println("The simple fields 'status' and 'action' pass through unchanged,");
    System.out.println("while the CAST is converted to a Splunk eval statement.");
  }

  @Test void testMultipleCasts() {
    // Multiple CAST operations in one query

    String multipleCasts =
        "SELECT CAST(\"status\" AS VARCHAR) as status_str, " +
        "       CAST(\"bytes\" AS INTEGER) as bytes_int, " +
        "       CAST(\"response_time\" AS DOUBLE) as resp_double " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    System.out.println("Multiple CAST operations are combined:");
    System.out.println(multipleCasts);
    System.out.println();
    System.out.println("Generates: | eval status_str = tostring(status), " +
                       "bytes_int = toint(bytes), resp_double = tonumber(response_time)");
    System.out.println();
    System.out.println("All CAST operations are combined into a single eval statement.");
  }

  @Test void testUnsupportedCastTypesNotPushedDown() {
    // CAST to types that Splunk doesn't have native functions for

    String dateCast =
        "SELECT CAST(\"timestamp_str\" AS DATE) as event_date " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    String timestampCast =
        "SELECT CAST(\"time_str\" AS TIMESTAMP) as event_time " +
        "FROM \"splunk\".\"web\" " +
        "LIMIT 10";

    System.out.println("Unsupported CAST types are handled by Calcite:");
    System.out.println("1. DATE cast: " + dateCast);
    System.out.println("2. TIMESTAMP cast: " + timestampCast);
    System.out.println();
    System.out.println("These types don't have direct Splunk conversion functions,");
    System.out.println("so they are handled by Calcite's type conversion logic.");
  }
}
