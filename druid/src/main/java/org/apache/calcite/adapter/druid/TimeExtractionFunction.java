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

import com.fasterxml.jackson.core.JsonGenerator;

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
  public static TimeExtractionFunction createDefault() {
    return new TimeExtractionFunction(ISO_TIME_FORMAT, null, "UTC", null);
  }

  /**
   * Creates the time format extraction function for the given granularity.
   * Only YEAR, MONTH, and DAY granularity are supported.
   *
   * @param granularity granularity to apply to the column
   * @return the time extraction function or null if granularity is not supported
   */
  public static TimeExtractionFunction createExtractFromGranularity(Granularity granularity) {
    switch (granularity) {
    case DAY:
      return new TimeExtractionFunction("d", null, "UTC", Locale.getDefault().toLanguageTag());
    case MONTH:
      return new TimeExtractionFunction("M", null, "UTC", Locale.getDefault().toLanguageTag());
    case YEAR:
      return new TimeExtractionFunction("yyyy", null, "UTC", Locale.getDefault().toLanguageTag());
    default:
      throw new AssertionError("Extraction " + granularity.value + " is not valid");
    }
  }

  /**
   * Creates time format floor time extraction function using a given granularity.
   *
   * @param granularity granularity to apply to the column
   * @return the time extraction function or null if granularity is not supported
   */
  public static TimeExtractionFunction createFloorFromGranularity(Granularity granularity) {
    return new TimeExtractionFunction(ISO_TIME_FORMAT, granularity.value, "UTC", Locale
        .getDefault().toLanguageTag());
  }
}

// End TimeExtractionFunction.java
