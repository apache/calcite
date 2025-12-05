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
package org.apache.calcite.util.format.postgresql;

import com.google.common.base.Strings;

import java.util.Locale;

/**
 * Casing styles that can be applied to a string.
 */
public enum CapitalizationEnum {
  ALL_UPPER,
  ALL_LOWER,
  CAPITALIZED;

  /**
   * Applies the casing style to a string. The string is treated as one word.
   *
   * @param s string to transform
   * @param locale Locale to use when transforming the string
   * @return s with the casing style applied
   */
  public String apply(String s, Locale locale) {
    switch (this) {
    case ALL_UPPER:
      return s.toUpperCase(locale);
    case ALL_LOWER:
      return s.toLowerCase(locale);
    default:
      if (Strings.isNullOrEmpty(s)) {
        return s;
      }

      return s.substring(0, 1).toUpperCase(locale) + s.substring(1).toLowerCase(locale);
    }
  }
}
