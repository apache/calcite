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

import java.util.function.Consumer;

/**
 * A format element in a format string. Knows how to parse and unparse itself.
 */
public interface FormatElement {

  String format(java.util.Date date);

  /**
   * Returns the description of an element.
   *
   * <p>For example, {@code %H} in MySQL represents the hour in 24-hour format (e.g., 00..23). This
   * method returns the string "The hour (24-hour clock) as a decimal number (00-23)" which is the
   * description of the FormatElementEnum {@code HH24}
   */
  String getDescription();

  /**
   * Applies a consumer to a format element.
   *
   * <p>For example, {@code %R} in Google SQL represents the hour in 24-hour format (e.g., 00..23)
   * followed by the minute as a decimal number. This method would return [HH24, ":", MI].
   */
  default void flatten(Consumer<FormatElement> consumer) {
    consumer.accept(this);
  }
}
