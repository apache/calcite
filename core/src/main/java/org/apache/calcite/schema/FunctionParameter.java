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
package net.hydromatic.optiq;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

/**
 * Parameter to a {@link Function}.
 *
 * <p>NOTE: We'd have called it {@code Parameter} but the overlap with
 * {@link java.lang.reflect.Parameter} was too confusing.</p>
 */
public interface FunctionParameter {
  /**
   * Zero-based ordinal of this parameter within the member's parameter
   * list.
   *
   * @return Parameter ordinal
   */
  int getOrdinal();

  /**
   * Name of the parameter.
   *
   * @return Parameter name
   */
  String getName();

  /**
   * Returns the type of this parameter.
   *
   * @param typeFactory Type factory to be used to create the type
   *
   * @return Parameter type.
   */
  RelDataType getType(RelDataTypeFactory typeFactory);
}

// End FunctionParameter.java
