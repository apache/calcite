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

import org.apache.calcite.linq4j.function.Experimental;

import java.util.function.Consumer;

/**
 * A format element in a format string. Knows how to parse and unparse itself.
 */
@Experimental
public interface FormatElement {

  /**
   * Formats a date to its appropriate string representation for the element.
   */
  void format(StringBuilder sb, java.util.Date date);

  /**
   * Adds the appropriate {@link java.text.SimpleDateFormat} pattern for the element to
   * provided StringBuilder.
   *
   * <p>Note that certain FormatElements may not have a Java equivalent.
   * In such cases, calling this method will throw an {@link UnsupportedOperationException}.
   * See {@link FormatElementEnum#Q} as an example.
   */
  void toPattern(StringBuilder sb) throws UnsupportedOperationException;

  /**
   * Returns the description of an element.
   *
   * <p>For example, {@code %H} in MySQL represents the hour in 24-hour format
   * (e.g., 00..23). This method returns the string "The hour (24-hour clock) as
   * a decimal number (00-23)", which is the description of
   * {@link FormatElementEnum#HH24}.
   */
  String getDescription();

  /**
   * Applies a consumer to a format element.
   */
  default void flatten(Consumer<FormatElement> consumer) {
    consumer.accept(this);
  }
}
