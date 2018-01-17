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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Preconditions;

import java.io.IOException;

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

/**
 * Factory class for {@link Granularity}.
 */
public class Granularities {

  private Granularities(){
    // Private constructor for utility class
  }

  public static Granularity allGranularity = new AllGranularity();

  public static Granularity createGranularity(TimeUnitRange timeUnit, String timeZone) {
    if (timeUnit == null) {
      return null;
    }
    switch (timeUnit) {
    case YEAR:
      return new PeriodGranularity(Granularity.Type.YEAR, "P1Y", timeZone);
    case QUARTER:
      return new PeriodGranularity(Granularity.Type.QUARTER, "P3M", timeZone);
    case MONTH:
      return new PeriodGranularity(Granularity.Type.MONTH, "P1M", timeZone);
    case WEEK:
      return new PeriodGranularity(Granularity.Type.WEEK, "P1W", timeZone);
    case DAY:
      return new PeriodGranularity(Granularity.Type.DAY, "P1D", timeZone);
    case HOUR:
      return new PeriodGranularity(Granularity.Type.HOUR, "PT1H", timeZone);
    case MINUTE:
      return new PeriodGranularity(Granularity.Type.MINUTE, "PT1M", timeZone);
    case SECOND:
      return new PeriodGranularity(Granularity.Type.SECOND, "PT1S", timeZone);
    default:
      return null;

    }
  }

  /**
   * AllGranularity for druid query.
   * This class is intended to generate a JSON fragment as part of a Druid query.
   * When used druid will rollup time values in a single bucket.
   */
  private static class AllGranularity implements Granularity {

    private AllGranularity() {
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", "all");
      generator.writeEndObject();
    }

    @Override public Type getType() {
      return Type.ALL;
    }
  }

  /**
   * PeriodGranularity of a druid query
   * This class is intended to generate a JSON fragment as part of a Druid query
   * When used druid will rollup and round time values based on specified period and timezone.
   */
  private static class PeriodGranularity implements Granularity {

    private final Type type;
    private final String period;
    private final String timeZone;

    private PeriodGranularity(Type type, String period, String timeZone) {
      this.type = Preconditions.checkNotNull(type);
      this.period = Preconditions.checkNotNull(period);
      this.timeZone = Preconditions.checkNotNull(timeZone);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", "period");
      writeFieldIf(generator, "period", period);
      writeFieldIf(generator, "timeZone", timeZone);
      generator.writeEndObject();
    }

    @Override public Type getType() {
      return type;
    }

  }
}

// End Granularities.java
