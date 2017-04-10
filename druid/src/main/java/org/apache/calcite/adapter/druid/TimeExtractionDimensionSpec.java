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

  public TimeExtractionDimensionSpec(
      ExtractionFunction extractionFunction, String outputName) {
    super(DruidTable.DEFAULT_TIMESTAMP_COLUMN, extractionFunction, outputName);
  }

  public static TimeExtractionDimensionSpec makeFullTimeExtract(String outputName) {
    return new TimeExtractionDimensionSpec(
        TimeExtractionFunction.createDefault(), outputName);
  }

  public static TimeExtractionDimensionSpec makeExtract(
      Granularity granularity, String outputName) {
    switch (granularity) {
    case YEAR:
      return new TimeExtractionDimensionSpec(
          TimeExtractionFunction.createFromGranularity(granularity), outputName);
    case MONTH:
      return new TimeExtractionDimensionSpec(
          TimeExtractionFunction.createFromGranularity(granularity), outputName);
    case DAY:
      return new TimeExtractionDimensionSpec(
          TimeExtractionFunction.createFromGranularity(granularity), outputName);
    default:
      return null;
    }
  }
}

// End TimeExtractionDimensionSpec.java
