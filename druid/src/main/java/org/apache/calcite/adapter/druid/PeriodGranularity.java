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

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

/** PeriodGranularity of a Druid query.
 * JSON representation for {@code io.druid.java.util.common.granularity.PeriodGranularity}
 */
public class PeriodGranularity implements Granularity {

  private final Type type;
  private final String period;
  private final String timeZone;

  public PeriodGranularity(Type type, String period, String timeZone) {
    this.type = type;
    this.period = period;
    this.timeZone = timeZone;
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

// End PeriodGranularity.java
