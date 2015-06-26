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
 * Custom table schema element.
 *
 * <p>Like base class {@link JsonTable},
 * occurs within {@link JsonMapSchema#tables}.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonCustomTable extends JsonTable {
  /** Name of the factory class for this table.
   *
   * <p>Required. Must implement interface
   * {@link org.apache.calcite.schema.TableFactory} and have a public default
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
}

// End JsonCustomTable.java
