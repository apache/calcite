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
package org.apache.calcite.adapter.file.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Utility class for handling null equivalent strings in CSV and other text formats.
 */
public final class NullEquivalents {

  /**
   * Default set of strings that are considered equivalent to NULL.
   * These are checked case-insensitively.
   */
  public static final Set<String> DEFAULT_NULL_EQUIVALENTS = new HashSet<>(Arrays.asList(
      "NULL",
      "NA",
      "N/A",
      "NONE",
      "NIL",
      ""
  ));

  private NullEquivalents() {
    // Utility class
  }

  /**
   * Check if a string value represents a null value using the default null equivalents.
   * @param value The string value to check
   * @return true if the value matches a null equivalent (case-insensitive)
   */
  public static boolean isNullRepresentation(String value) {
    return isNullRepresentation(value, DEFAULT_NULL_EQUIVALENTS);
  }

  /**
   * Check if a string value represents a null value using a custom set of null equivalents.
   * @param value The string value to check
   * @param nullEquivalents Set of strings (case-insensitive) that represent null
   * @return true if the value matches a null equivalent (case-insensitive)
   */
  public static boolean isNullRepresentation(String value, Set<String> nullEquivalents) {
    if (value == null) {
      return false; // null is already null, not a "representation" of null
    }

    // Trim the value
    String trimmed = value.trim();

    // Empty or blank strings are considered null representations for type inference
    // (since they can't be parsed as numbers, dates, etc.)
    if (trimmed.isEmpty()) {
      return true;
    }

    // Check if it matches any explicit null markers
    String upper = trimmed.toUpperCase(Locale.ROOT);
    return nullEquivalents.contains(upper);
  }
}
