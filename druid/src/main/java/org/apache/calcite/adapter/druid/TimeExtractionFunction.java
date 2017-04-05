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

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

/**
 * Time extraction implementation
 */
public class TimeExtractionFunction implements ExtractionFunction {

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
    writeFieldIf(generator, "local", local);
    generator.writeEndObject();
  }

  public static TimeExtractionFunction createDefault() {
    return new TimeExtractionFunction("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", null, "UTC", null);
  }

  public static TimeExtractionFunction createFromGranularity(Granularity granularity) {
    switch (granularity) {
    case DAY:
      return new TimeExtractionFunction("dd", null, "UTC", null);
    case MONTH:
      return new TimeExtractionFunction("MM", null, "UTC", null);
    case YEAR:
      return new TimeExtractionFunction("yyyy", null, "UTC", null);
    case HOUR:
      return new TimeExtractionFunction("hh", null, "UTC", null);
    default:
      throw new AssertionError("Extraction " + granularity.value + " is not valid");
    }
  }
}

// End TimeExtractionFunction.java
