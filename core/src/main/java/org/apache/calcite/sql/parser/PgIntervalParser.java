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
package org.apache.calcite.sql.parser;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.StringTokenizer;

/**
 * Parser for PostgreSQL intervals.
 *
 * Based on <a href="https://github.com/crate/crate/blob/b8f5bfc1ae1d34b0426f2a01217fde663505225b/server/src/main/java/io/crate/interval/PGIntervalParser.java">Crate.io</a>.
 */
final class PgIntervalParser {
  private PgIntervalParser() {}

  static PgInterval parse(String value) {
    boolean dataParsed = false;

    int years = 0;
    int months = 0;
    int days = 0;
    int weeks = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    int milliSeconds = 0;

    try {
      String valueToken = null;

      value = value.replace('+', ' ').replace('@', ' ');
      final StringTokenizer st = new StringTokenizer(value);
      for (int i = 1; st.hasMoreTokens(); i++) {
        String token = st.nextToken();

        if ((i & 1) == 1) {
          int endHours = token.indexOf(':');
          if (endHours == -1) {
            valueToken = token;
            continue;
          }
          // This handles hours, minutes, seconds and microseconds for
          // ISO intervals
          int offset = (token.charAt(0) == '-') ? 1 : 0;

          hours = nullSafeIntGet(token.substring(offset, endHours));
          minutes = nullSafeIntGet(token.substring(endHours + 1, endHours + 3));

          int endMinutes = token.indexOf(':', endHours + 1);
          seconds = nullSafeIntGet(token.substring(endMinutes + 1));
          milliSeconds = parseMilliSeconds(token.substring(endMinutes + 1));

          if (offset == 1) {
            hours = -hours;
            minutes = -minutes;
            seconds = -seconds;
            milliSeconds = -milliSeconds;
          }
          valueToken = null;
        } else {
          // This handles years, months, days for both, ISO and
          // non-ISO intervals. Hours, minutes, seconds and microseconds
          // are handled for Non-ISO intervals here.
          if (token.startsWith("year") || token.equals("yr")) {
            years = nullSafeIntGet(valueToken);
          } else if (token.startsWith("mon")) {
            months = nullSafeIntGet(valueToken);
          } else if (token.startsWith("day") || token.equals("d")) {
            days = nullSafeIntGet(valueToken);
          } else if (token.startsWith("week")) {
            weeks = nullSafeIntGet(valueToken);
          } else if (token.startsWith("hour") || token.startsWith("hr")) {
            hours = nullSafeIntGet(valueToken);
          } else if (token.startsWith("min")) {
            minutes = nullSafeIntGet(valueToken);
          } else if (token.startsWith("sec")) {
            seconds = nullSafeIntGet(valueToken);
            milliSeconds = parseMilliSeconds(valueToken);
          } else if (token.startsWith("milli")) {
            milliSeconds += nullSafeIntGet(valueToken);
          } else {
            throw new IllegalArgumentException("Unexpected token: " + token);
          }
          dataParsed = true;
        }
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid interval format " + value);
    }

    if (!dataParsed) {
      throw new IllegalArgumentException("Invalid interval format " + value);
    }

    boolean present = !value.endsWith(" ago");

    return new PgInterval(
        present,
        years,
        months,
        weeks * 7 + days,
        hours,
        minutes,
        seconds,
        milliSeconds);
  }

  static int parseMilliSeconds(@Nullable String value) throws NumberFormatException {
    if (value == null) {
      return 0;
    } else {
      BigDecimal decimal = new BigDecimal(value);
      return decimal
          .subtract(new BigDecimal(decimal.intValue()))
          .multiply(new BigDecimal(1000)).intValue();
    }
  }

  static int nullSafeIntGet(@Nullable String value) {
    return (value == null) ? 0 : Integer.parseInt(value);
  }
}
