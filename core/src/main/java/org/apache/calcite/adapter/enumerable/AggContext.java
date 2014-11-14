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
package net.hydromatic.optiq.rules.java;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Information on the aggregate calculation context.
 * {@link AggAddContext} provides basic static information on types of arguments
 * and the return value of the aggregate being implemented.
 */
public interface AggContext {
  /**
   * Returns the aggregation being implemented.
   * @return aggregation being implemented.
   */
  Aggregation aggregation();

  /**
   * Returns the return type of the aggregate as {@link org.eigenbase.reltype.RelDataType}.
   * This can be helpful to test {@link org.eigenbase.reltype.RelDataType#isNullable()}.
   * @return return type of the aggregate as {@link org.eigenbase.reltype.RelDataType}
   */
  RelDataType returnRelType();

  /**
   * Returns the return type of the aggregate as {@link java.lang.reflect.Type}.
   * @return return type of the aggregate as {@link java.lang.reflect.Type}
   */
  Type returnType();

  /**
   * Returns the parameter types of the aggregate as {@link org.eigenbase.reltype.RelDataType}.
   * This can be helpful to test {@link org.eigenbase.reltype.RelDataType#isNullable()}.
   * @return parameter types of the aggregate as {@link org.eigenbase.reltype.RelDataType}
   */
  List<? extends RelDataType> parameterRelTypes();

  /**
   * Returns the parameter types of the aggregate as {@link java.lang.reflect.Type}.
   * @return parameter types of the aggregate as {@link java.lang.reflect.Type}
   */
  List<? extends Type> parameterTypes();
}
