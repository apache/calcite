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
package org.apache.calcite.util.format;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;

/**
 * A format element in a format string. Knows how to parse and unparse itself.
 */
public interface FormatModelElement {
  /** TODO(CALCITE-2980): use these methods when implementing format functions. */
  // String unparseFromDate(java.util.Date date);
  // String unparseFromInt(Integer date);
  // String unparseFromLong(Long timestamp);

  /**
   * Returns the literal value of an element.
   *
   * <p>For example, {@code %H} in MySQL represents the hour in 24-hour format (e.g., 00..23). This
   * method returns the string literal "%H".
   */
  String getLiteral();

  /**
   * Returns the {@link FormatElementEnum} token representing the element.
   *
   * <p>For example, {@code %H} in MySQL represents the hour in 24-hour format (e.g., 00..23). This
   * method returns the string "HH24" which is the name of the FormatElementEnum {@code HH24}
   */
  String getToken();

  /**
   * Returns the description of an element.
   *
   * <p>For example, {@code %H} in MySQL represents the hour in 24-hour format (e.g., 00..23). This
   * method returns the string "The hour (24-hour clock) as a decimal number (00-23)" which is the
   * description of the FormatElementEnum {@code HH24}
   */
  String getDescription();

  /**
   * Whether this element is a {@link FormatModelElementAlias}. An alias comprises one or
   * more standard elements declared in {@link FormatElementEnum}.
   *
   * <p>For example, {@code %R} in Google SQL represents the hour in 24-hour format (e.g., 00..23)
   * followed by the minute as a decimal number. This method would return {@code true} as "%R" is
   * an alias for the standard elements {@code HH24} and {@code MI}.
   */
  default boolean isAlias() {
    return false;
  };

  /**
   * Returns the composite format elements for an alias. If not an alias, returns itself as a list.
   *
   * <p>For example, {@code %R} in Google SQL represents the hour in 24-hour format (e.g., 00..23)
   * followed by the minute as a decimal number. This method would return [HH24, ":", MI].
   */
  default List<FormatModelElement> getElements() {
    return Arrays.asList(this);
  };

  /**
   * Convenience method to generate a parse map from a list of {@link FormatModelElement}.
   *
   * <p>Keys are the value of {@link FormatModelElement#getLiteral()} and values are the
   * FormatModelElement they represent.</p>
   */
  static ImmutableMap<String, FormatModelElement> listToMap(List<FormatModelElement> elementList) {
    return Maps.uniqueIndex(elementList, FormatModelElement::getLiteral);
  }
}
