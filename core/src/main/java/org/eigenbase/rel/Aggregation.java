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
package org.eigenbase.rel;

import java.util.List;

import org.eigenbase.reltype.*;

/**
 * An <code>Aggregation</code> aggregates a set of values into one value.
 *
 * <p>It is used, via a {@link AggregateCall}, in an {@link AggregateRel}
 * relational operator.</p>
 */
public interface Aggregation {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the parameter types accepted by this Aggregation.
   *
   * @param typeFactory Type factory to create the types
   * @return Array of parameter types
   */
  List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory);

  /**
   * Returns the type of the result yielded by this Aggregation.
   *
   * @param typeFactory Type factory to create the type
   * @return Result type
   */
  RelDataType getReturnType(RelDataTypeFactory typeFactory);

  /**
   * Returns the name of this Aggregation
   *
   * @return name of this aggregation
   */
  String getName();
}

// End Aggregation.java
