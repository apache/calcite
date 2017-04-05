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

/**
 * Time extraction dimension spec implementation
 */
public class TimeExtractionDimensionSpec extends ExtractionDimensionSpec {
  private final String outputName;

  public TimeExtractionDimensionSpec(ExtractionFunction extractionFunction,
          String outputName
  ) {
    super("__time", extractionFunction, outputName);
    this.outputName = outputName;
  }

  public static TimeExtractionDimensionSpec makeFullTimeExtract() {
    return new TimeExtractionDimensionSpec(
            TimeExtractionFunction.createDefault(),
            DruidConnectionImpl.DEFAULT_RESPONSE_TIMESTAMP_COLUMN
    );
  }

  public String getOutputName() {
    return outputName;
  }

  public static TimeExtractionDimensionSpec makeExtract(Granularity granularity) {
    switch (granularity) {
    case YEAR:
      return new TimeExtractionDimensionSpec(
              TimeExtractionFunction.createFromGranularity(granularity), "year");
    case MONTH:
      return new TimeExtractionDimensionSpec(
              TimeExtractionFunction.createFromGranularity(granularity), "monthOfYear");
    case DAY:
      return new TimeExtractionDimensionSpec(
            TimeExtractionFunction.createFromGranularity(granularity), "dayOfMonth");
    default:
      return null;
    }
  }
}

// End TimeExtractionDimensionSpec.java
