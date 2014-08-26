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
package org.eigenbase.sarg;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * SargExpr represents an expression defining a possibly non-contiguous search
 * subset of a scalar domain of a given datatype.
 */
public interface SargExpr {
  //~ Methods ----------------------------------------------------------------

  /**
   * Overrides the default Object.toString. The result must be safe for use in
   * a RelNode digest.
   */
  String toString();

  /**
   * @return datatype for coordinates of search domain
   */
  RelDataType getDataType();

  /**
   * Resolves this expression into a fixed {@link SargIntervalSequence}.
   *
   * <p>TODO jvs 17-Jan-2006: add binding for dynamic params so they can be
   * evaluated as well
   *
   * @return immutable ordered sequence of disjoint intervals
   */
  SargIntervalSequence evaluate();

  /**
   * Resolves the complement of this expression into a fixed {@link
   * SargIntervalSequence}.
   *
   * @return immutable ordered sequence of disjoint intervals
   */
  SargIntervalSequence evaluateComplemented();

  /**
   * @return the factory which produced this expression
   */
  SargFactory getFactory();

  /**
   * Collects all dynamic parameters referenced by this expression.
   *
   * @param dynamicParams receives dynamic parameter references
   */
  void collectDynamicParams(Set<RexDynamicParam> dynamicParams);
}

// End SargExpr.java
