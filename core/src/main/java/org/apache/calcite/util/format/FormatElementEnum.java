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

/**
 * Implementation of {@link FormatModelElement} containing the standard format elements. These are
 * based on
 * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Format-Models.html">
 * Oracle's format model documentation.</a>
 */
public enum FormatElementEnum implements FormatModelElement {
  D("The weekday (Monday as the first day of the week) as a decimal number (1-7)"),
  DAY("The full weekday name"),
  DD("The day of the month as a decimal number (01-31)"),
  DDD("The day of the year as a decimal number (001-366)"),
  DY("The abbreviated weekday name"),
  HH24("The hour (24-hour clock) as a decimal number (00-23)"),
  IW("The ISO 8601 week number of the year (Monday as the first day of the week) "
      + "as a decimal number (01-53)"),
  MI("The minute as a decimal number (00-59)"),
  MM("The month as a decimal number (01-12)"),
  MON("The abbreviated month name"),
  MONTH("The full month name (English)"),
  Q("The quarter as a decimal number (1-4)"),
  SS("The second as a decimal number (00-60)"),
  TZR("The time zone name"),
  WW("The week number of the year (Sunday as the first day of the week) as a decimal "
      + "number (00-53)"),
  YYYY("The year with century as a decimal number");

  private final String description;

  FormatElementEnum(String description) {
    this.description = description;
  }

  @Override public String getLiteral() {
    // For these standard elements, their literal and "tokenized" representation are the same
    return getToken();
  }

  @Override public String getToken() {
    return this.name();
  }

  @Override public String getDescription() {
    return description;
  }
}
