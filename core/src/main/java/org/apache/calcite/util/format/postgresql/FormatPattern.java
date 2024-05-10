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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.ParsePosition;
import java.time.ZonedDateTime;

/**
 * A format element that is able to produce a string from a date.
 */
public interface FormatPattern {
  /**
   * Checks if this pattern matches the substring starting at the <code>parsePosition</code>
   * in the <code>formatString</code>. If it matches, then the <code>dateTime</code> is
   * converted to a string based on this pattern. For example, "YYYY" will get the year of
   * the <code>dateTime</code> and convert it to a string.
   *
   * @param parsePosition current position in the format string
   * @param formatString input format string
   * @param dateTime datetime to convert
   * @return the string representation of the datetime based on the format pattern
   */
  @Nullable String convert(ParsePosition parsePosition, String formatString,
      ZonedDateTime dateTime);
}
