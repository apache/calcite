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
package org.apache.calcite.util.format.postgresql.format.compiled;

import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * A single component of a parsed date/time format. Can be a pattern such as
 * "YYYY" or a literal string. Able to output the string representation of a
 * date/time component.
 */
public interface CompiledItem {
  /**
   * Creates one portion of the formatted date/time.
   *
   * @param dateTime date/time value to format
   * @param locale Locale to use for day or month names if the TM modifier was present
   * @return the formatted String value of this date/time component
   */
  String convertToString(ZonedDateTime dateTime, Locale locale);

  /**
   * Returns the length of the format pattern.
   *
   * @return length of the format pattern
   */
  int getFormatPatternLength();
}
