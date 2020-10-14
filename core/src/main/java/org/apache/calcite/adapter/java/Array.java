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
package org.apache.calcite.adapter.java;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * Annotation that indicates that a field is an array type.
 *
 * 代表该字段是个list。
 */
@Target({FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Array {

  /**
   * Component type.
   *
   * 组件类型。
   */
  Class component();

  /**
   * Whether components may be null.
   *
   * 组件是否可以为null。
   */
  boolean componentIsNullable() default false;

  /**
   * Maximum number of elements in the array. -1 means no maximum.
   *
   * 数组的最大容量，-1表示没有限制。
   */
  long maximumCardinality() default -1L;
}
