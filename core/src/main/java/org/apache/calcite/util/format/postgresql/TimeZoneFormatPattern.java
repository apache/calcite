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

import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;

/**
 * Able to parse timezone codes from string and to get the timezone from a datetime.
 * Timezone codes are 3 letters, such as PST or UTC.
 */
public class TimeZoneFormatPattern extends StringFormatPattern {
  final boolean isUpperCase;

  public TimeZoneFormatPattern(boolean isUpperCase, String... patterns) {
    super(patterns);
    this.isUpperCase = isUpperCase;
  }

  @Override String dateTimeToString(ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable String suffix, Locale locale) {

    final String zoneCode = dateTime.getZone().getDisplayName(TextStyle.SHORT, locale);
    return String.format(
        locale,
        "%3s",
        isUpperCase ? zoneCode.toUpperCase(locale) : zoneCode.toLowerCase(locale));
  }
}
