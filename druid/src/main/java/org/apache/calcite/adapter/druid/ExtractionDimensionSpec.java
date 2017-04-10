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
 * Extraction function dimension spec implementation
 */
public class ExtractionDimensionSpec implements DimensionSpec {
  private final String dimension;
  private final ExtractionFunction extractionFunction;
  private final String outputName;

  public ExtractionDimensionSpec(String dimension, ExtractionFunction extractionFunction,
          String outputName
  ) {
    this.dimension = dimension;
    this.extractionFunction = extractionFunction;
    this.outputName = outputName;
  }

  @Override public void write(JsonGenerator generator) throws IOException {

    generator.writeStartObject();
    generator.writeStringField("type", "extraction");
    generator.writeStringField("dimension", dimension);
    writeFieldIf(generator, "outputName", outputName);
    writeFieldIf(generator, "extractionFn", extractionFunction);
    generator.writeEndObject();
  }

}

// End ExtractionDimensionSpec.java
