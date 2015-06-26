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
package org.apache.calcite.model;

import java.util.Map;

/**
 * JSON schema element that represents a custom schema.
 *
 * <p>Like the base class {@link JsonSchema},
 * occurs within {@link JsonRoot#schemas}.
 *
 * @see org.apache.calcite.model.JsonRoot Description of schema elements
 */
public class JsonCustomSchema extends JsonMapSchema {
  /** Name of the factory class for this schema.
   *
   * <p>Required. Must implement interface
   * {@link org.apache.calcite.schema.SchemaFactory} and have a public default
   * constructor.
   */
  public String factory;

  /** Contains attributes to be passed to the factory.
   *
   * <p>May be a JSON object (represented as Map) or null.
   */
  public Map<String, Object> operand;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public String toString() {
    return "JsonCustomSchema(name=" + name + ")";
  }
}

// End JsonCustomSchema.java
