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
import java.util.Objects;

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

/**
 * Druid Json Expression based Virtual Column.
 * Virtual columns is used as "projection" concept throughout Druid using expression.
 */
public class VirtualColumn implements DruidJson {
  private final String name;

  private final String expression;

  private final DruidType outputType;

  public VirtualColumn(String name, String expression, DruidType outputType) {
    this.name = Objects.requireNonNull(name);
    this.expression = Objects.requireNonNull(expression);
    this.outputType = outputType == null ? DruidType.FLOAT : outputType;
  }

  @Override public void write(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("type", "expression");
    generator.writeStringField("name", name);
    generator.writeStringField("expression", expression);
    writeFieldIf(generator, "outputType", getOutputType().toString().toUpperCase(Locale.ENGLISH));
    generator.writeEndObject();
  }

  public String getName() {
    return name;
  }

  public String getExpression() {
    return expression;
  }

  public DruidType getOutputType() {
    return outputType;
  }

  /**
   * Virtual Column Builder
   */
  public static class Builder {
    private String name;

    private String expression;

    private DruidType type;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withExpression(String expression) {
      this.expression = expression;
      return this;
    }

    public Builder withType(DruidType type) {
      this.type = type;
      return this;
    }

    public VirtualColumn build() {
      return new VirtualColumn(name, expression, type);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}

// End VirtualColumn.java
