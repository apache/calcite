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

import java.sql.Time;

public class DebugTimeHandling {

  @Test public void debugTimeValues() {
    System.out.println("DEBUG: Time Value Analysis");
    System.out.println("==========================");

    // The test expects "00:01:02" but gets "19:01:02"
    // Let's understand why

    // millisSinceMidnight for "00:01:02" = 1*60*1000 + 2*1000 = 62000
    int millisSinceMidnight = 62000;

    // Create Time using the constructor (what our code does)
    Time timeFromConstructor = new Time(millisSinceMidnight);
    System.out.println("Time from constructor new Time(62000): " + timeFromConstructor);

    // Create Time using valueOf (what the test expects)
    Time timeFromValueOf = Time.valueOf("00:01:02");
    System.out.println("Time from valueOf(\"00:01:02\"): " + timeFromValueOf);

    // Check their millisecond values
    System.out.println("\nMillisecond values:");
    System.out.println("Constructor time millis: " + timeFromConstructor.getTime());
    System.out.println("valueOf time millis: " + timeFromValueOf.getTime());

    // The issue: Time constructor expects milliseconds since epoch (Jan 1, 1970 00:00:00 GMT)
    // But we're passing milliseconds since midnight
    System.out.println("\nThe problem:");
    System.out.println("62000 ms = 62 seconds = 1 minute 2 seconds");
    System.out.println("But Time(62000) interprets this as 62 seconds after epoch");
    System.out.println("Which is Jan 1, 1970 00:01:02 GMT");
    System.out.println("In EST (-5 hours), that's Dec 31, 1969 19:01:02");
  }
}
