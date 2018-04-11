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

import java.util.ArrayList;
import java.util.List;

/**
 * JSON object representing a schema whose tables are explicitly specified.
 *
 * <p>Like the base class {@link JsonSchema},
 * occurs within {@link JsonRoot#schemas}.
 *
 * @see JsonRoot Description of JSON schema elements
 */
public class JsonMapSchema extends JsonSchema {
  /** Tables in this schema.
   *
   * <p>The list may be empty.
   */
  public final List<JsonTable> tables = new ArrayList<>();

  /** Types in this schema.
   *
   * <p>The list may be empty.
   */
  public final List<JsonType> types = new ArrayList<>();

  /** Functions in this schema.
   *
   * <p>The list may be empty.
   */
  public final List<JsonFunction> functions = new ArrayList<>();

  @Override public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public void visitChildren(ModelHandler modelHandler) {
    super.visitChildren(modelHandler);
    for (JsonTable jsonTable : tables) {
      jsonTable.accept(modelHandler);
    }
    for (JsonFunction jsonFunction : functions) {
      jsonFunction.accept(modelHandler);
    }
    for (JsonType jsonType : types) {
      jsonType.accept(modelHandler);
    }
  }
}

// End JsonMapSchema.java
