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
package org.apache.calcite.test;

import org.apache.calcite.adapter.java.ReflectiveSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A ReflectiveSchema that does not return row count statistics.
 * Intended to be used with tests written before row count statistics were supported
 * by ReflectiveSchema that rely on legacy behavior.
 */
public class ReflectiveSchemaWithoutRowCount extends ReflectiveSchema  {
  /**
   * Creates a ReflectiveSchema.
   *
   * @param target Object whose fields will be sub-objects of the schema
   */
  public ReflectiveSchemaWithoutRowCount(Object target) {
    super(target);
  }

  @Override protected @Nullable Double getRowCount(Object o) {
    return null;
  }
}
