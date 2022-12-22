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
package org.apache.calcite.sql.type;

import java.util.Locale;

/**
 * Class that serves a struct for the tz-info needed to generate proper
 * Bodo code. This hold the tz information as well as handling whether the
 * timezone is a string, integer, or some other type.
 */
public class BodoTZInfo {
  // The zone value used in the .tz field in Bodo's
  // compiler.
  private final String zone;
  // What type is the zone in Python. This is
  // used for generating Python code. This is used
  // to avoid error with trying to infer if the
  // zone should be an integer.
  private final String pyType;

  public static final BodoTZInfo UTC = new BodoTZInfo("UTC", "str");

  public BodoTZInfo(String zone, String pyType) {
    this.zone = zone;
    // Standardize to lower case.
    this.pyType = pyType.toLowerCase(Locale.ROOT);
  }

  /**
   * Get the zone information as it will be passed to Python.
   *
   * @return A string of the generated Python code for
   * the zone.
   */
  public String getPyZone() {
    if (pyType.equals("str")) {
      // Append single quotes for Python strings.
      return String.format(Locale.ROOT, "'%s'", zone);
    } else {
      assert pyType.equals("int");
      return zone;
    }
  }



}
