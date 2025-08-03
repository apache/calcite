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
package org.apache.calcite.adapter.file.debug;

import org.junit.jupiter.api.Test;

public class TimestampFailureAnalysis {

  @Test public void analyzeFailures() {
    System.out.println("TIMESTAMP TEST FAILURE ANALYSIS");
    System.out.println("===============================\n");

    System.out.println("1. testDateType2() - FORMATTING ISSUE ONLY:");
    System.out.println("   Expected: \"2015-12-31 07:15:56\"");
    System.out.println("   Actual:   \"2015-12-31 07:15:56.000\"");
    System.out.println("   Issue: Extra .000 milliseconds in string format");
    System.out.println("   ✅ Timestamp value is CORRECT, only formatting differs\n");

    System.out.println("2. testCsvGroupByTimestampAdd() & testGroupByTimestampAdd() - DATE ARITHMETIC ISSUE:");
    System.out.println("   Expected dates are 1 day ahead:");
    System.out.println("   - Expected: 1996-08-04, Actual: 1996-08-03");
    System.out.println("   - Expected: 2001-01-02, Actual: 2001-01-01");
    System.out.println("   Issue: TIMESTAMPADD function might not be handling timezone correctly\n");

    System.out.println("3. testFilterOnNullableTimestamp() - DATE OFF BY 1:");
    System.out.println("   Expected: 1996-08-03");
    System.out.println("   Actual:   1996-08-02");
    System.out.println("   Issue: Date filtering is showing previous day\n");

    System.out.println("4. testFilterOnNullableTimestamp2() - YEAR OFF BY 1:");
    System.out.println("   Expected: 2007");
    System.out.println("   Actual:   2006");
    System.out.println("   Issue: Year extraction from timestamp is off\n");

    System.out.println("5. testTimestampGroupByAndOrderBy() - NULL HANDLING:");
    System.out.println("   Expected: 1996-08-03 00:01:02.0");
    System.out.println("   Actual:   1969-12-31 19:00:00.0 (epoch - timezone offset)");
    System.out.println("   Issue: NULL timestamps being converted to epoch with timezone issues\n");

    System.out.println("6. testTimestampOrderBy() - NULL HANDLING:");
    System.out.println("   Expected: 1996-08-03 00:01:02.0");
    System.out.println("   Actual:   1970-01-01 00:00:00.0 (epoch)");
    System.out.println("   Issue: Similar NULL handling but different result\n");

    System.out.println("\nSUMMARY:");
    System.out.println("- 1 pure formatting issue (testDateType2)");
    System.out.println("- 2 date arithmetic issues (TIMESTAMPADD)");
    System.out.println("- 3 date boundary issues (dates showing 1 day earlier)");
    System.out.println("- 2 NULL timestamp handling issues");
    System.out.println("- 1 year extraction issue");
  }
}
