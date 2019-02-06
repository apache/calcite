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
package org.apache.calcite.rel.type;

import java.util.Map;

/**
 * RelDataTypeField represents the definition of a field in a structured
 * {@link RelDataType}.
 *
 * <p>Extends the {@link java.util.Map.Entry} interface to allow convenient
 * inter-operation with Java collections classes. In any implementation of this
 * interface, {@link #getKey()} must be equivalent to {@link #getName()}
 * and {@link #getValue()} must be equivalent to {@link #getType()}.
 */
public interface RelDataTypeField extends Map.Entry<String, RelDataType> {

  /**
   * Function to transform a set of {@link RelDataTypeField} to
   * a set of {@link Integer} of the field keys.
   *
   * @deprecated Use {@code RelDataTypeField::getIndex}
   */
  @Deprecated // to be removed before 2.0
  class ToFieldIndex
      implements com.google.common.base.Function<RelDataTypeField, Integer> {
    @Override public Integer apply(RelDataTypeField o) {
      return o.getIndex();
    }
  }

  /**
   * Function to transform a set of {@link RelDataTypeField} to
   * a set of {@link String} of the field names.
   *
   * @deprecated Use {@code RelDataTypeField::getName}
   */
  @Deprecated // to be removed before 2.0
  class ToFieldName
      implements com.google.common.base.Function<RelDataTypeField, String> {
    @Override public String apply(RelDataTypeField o) {
      return o.getName();
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Gets the name of this field, which is unique within its containing type.
   *
   * @return field name
   */
  String getName();

  /**
   * Gets the ordinal of this field within its containing type.
   *
   * @return 0-based ordinal
   */
  int getIndex();

  /**
   * Gets the type of this field.
   *
   * @return field type
   */
  RelDataType getType();

  /**
   * Returns true if this is a dynamic star field.
   */
  boolean isDynamicStar();
}

// End RelDataTypeField.java
