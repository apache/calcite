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
import java.time.temporal.ChronoField;
import java.util.Locale;

/**
 * Able to parse timezone minutes from string and to generate a string of the timezone
 * minutes from a datetime. Timezone minutes always have two digits and are between
 * 00 and 59.
 */
public class TimeZoneMinutesFormatPattern extends StringFormatPattern {
  public TimeZoneMinutesFormatPattern() {
    super("TZM");
  }

  @Override String dateTimeToString(final ZonedDateTime dateTime, boolean haveFillMode,
      @Nullable final String suffix, final Locale locale) {
    return String.format(
        Locale.ROOT,
        "%02d",
        (dateTime.getOffset().get(ChronoField.OFFSET_SECONDS) % 3600) / 60);
  }
}
