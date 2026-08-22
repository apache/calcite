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
package org.apache.calcite.adapter.milvus.operation;

/**
 * Milvus project expression abstraction representing different types of
 * projected values in a Milvus query.
 */
public abstract class MilvusProjectExpression {

  /**
   * Type of project expression.
   */
  public enum ExpressionType {
    /**
     * Constant value expression.
     */
    CONSTANT,
    /**
     * Vector retrieval score expression.
     */
    VECTOR_SCORE,
    /**
     * Input field expression.
     */
    INPUT_FIELD
  }

  private final Class<?> clazz;
  private final ExpressionType type;

  protected MilvusProjectExpression(ExpressionType type, Class<?> clazz) {
    this.clazz = clazz;
    this.type = type;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public ExpressionType getType() {
    return type;
  }

  /**
   * Constant value expression.
   */
  public static class Constant extends MilvusProjectExpression {
    private final Object value;

    public Constant(Class<?> clazz, Object value) {
      super(ExpressionType.CONSTANT, clazz);
      this.value = value;
    }

    public Object getValue() {
      return value;
    }

    // tostring
    @Override public String toString() {
      return "Constant{" + "value=" + value + ", clazz=" + getClass() + "}";
    }
  }

  /**
   * Vector retrieval score expression.
   */
  public static class VectorScore extends MilvusProjectExpression {
    public VectorScore(Class<?> clazz) {
      super(ExpressionType.VECTOR_SCORE, clazz);
    }

    @Override public String toString() {
      return "VectorScore{clazz=" + getClass() + "}";
    }
  }

  /**
   * Input field expression.
   */
  public static class InputField extends MilvusProjectExpression {
    private final String fieldName;

    public InputField(String fieldName, Class<?> clazz) {
      super(ExpressionType.INPUT_FIELD, clazz);
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override public String toString() {
      return "InputField{" + "fieldName='" + fieldName + '\'' + ", clazz=" + getClass() + "}";
    }
  }
}
