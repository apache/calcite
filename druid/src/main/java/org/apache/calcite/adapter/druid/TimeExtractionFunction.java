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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Locale;

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

/**
 * Implementation of Druid time format extraction function.
 *
 * <p>These functions return the dimension value formatted according to the given format string,
 * time zone, and locale.
 *
 * <p>For __time dimension values, this formats the time value bucketed by the aggregation
 * granularity.
 */
public class TimeExtractionFunction implements ExtractionFunction {

  private static final ImmutableSet<TimeUnitRange> VALID_TIME_EXTRACT = Sets.immutableEnumSet(
      TimeUnitRange.YEAR,
      TimeUnitRange.MONTH,
      TimeUnitRange.DAY,
      TimeUnitRange.WEEK);

  private static final String ISO_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  private final String format;
  private final String granularity;
  private final String timeZone;
  private final String local;

  public TimeExtractionFunction(String format, String granularity, String timeZone, String local) {
    this.format = format;
    this.granularity = granularity;
    this.timeZone = timeZone;
    this.local = local;
  }

  @Override public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("type", "timeFormat");
    writeFieldIf(generator, "format", format);
    writeFieldIf(generator, "granularity", granularity);
    writeFieldIf(generator, "timeZone", timeZone);
    writeFieldIf(generator, "locale", local);
    generator.writeEndObject();
  }

  /**
   * Creates the default time format extraction function.
   *
   * @return the time extraction function
   */
  public static TimeExtractionFunction createDefault(String timeZone) {
    return new TimeExtractionFunction(ISO_TIME_FORMAT, null, timeZone, null);
  }

  /**
   * Creates the time format extraction function for the given granularity.
   * Only YEAR, MONTH, and DAY granularity are supported.
   *
   * @param granularity granularity to apply to the column
   * @return the time extraction function corresponding to the granularity input unit
   * {@link TimeExtractionFunction#VALID_TIME_EXTRACT} for supported granularity
   */
  public static TimeExtractionFunction createExtractFromGranularity(
      Granularity granularity, String timeZone) {
    switch (granularity) {
    case DAY:
      return new TimeExtractionFunction("d", null, timeZone, Locale.getDefault().toLanguageTag());
    case MONTH:
      return new TimeExtractionFunction("M", null, timeZone, Locale.getDefault().toLanguageTag());
    case YEAR:
      return new TimeExtractionFunction("yyyy", null, timeZone,
          Locale.getDefault().toLanguageTag());
    case WEEK:
      return new TimeExtractionFunction("w", null, timeZone, Locale.getDefault().toLanguageTag());
    default:
      throw new IllegalArgumentException("Granularity [" + granularity + "] is not supported");
    }
  }

  /**
   * Creates time format floor time extraction function using a given granularity.
   *
   * @param granularity granularity to apply to the column
   * @return the time extraction function or null if granularity is not supported
   */
  public static TimeExtractionFunction createFloorFromGranularity(
      Granularity granularity, String timeZone) {
    return new TimeExtractionFunction(ISO_TIME_FORMAT, granularity.value, timeZone, Locale
        .getDefault().toLanguageTag());
  }

  /**
   * Returns whether the RexCall contains a valid extract unit that we can
   * serialize to Druid.
   *
   * @param call Extract expression
   *
   * @return true if the extract unit is valid
   */
  public static boolean isValidTimeExtract(RexCall call) {
    if (call.getKind() != SqlKind.EXTRACT) {
      return false;
    }
    final RexLiteral flag = (RexLiteral) call.operands.get(0);
    final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
    return timeUnit != null && VALID_TIME_EXTRACT.contains(timeUnit);
  }
}

// End TimeExtractionFunction.java
