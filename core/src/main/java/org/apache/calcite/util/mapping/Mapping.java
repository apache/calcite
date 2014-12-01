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
package org.apache.calcite.util.mapping;

import java.util.Iterator;

/**
 * A <dfn>Mapping</dfn> is a relationship between a source domain to target
 * domain of integers.
 *
 * <p>This interface represents the most general possible mapping. Depending on
 * the {@link MappingType} of a particular mapping, some of the operations may
 * not be applicable. If you call the method, you will receive a runtime error.
 * For instance:
 *
 * <ul>
 * <li>If a target has more than one source, then the method
 *     {@link #getSource(int)} will throw
 *     {@link Mappings.TooManyElementsException}.
 * <li>If a source has no targets, then the method {@link #getTarget} will throw
 *     {@link Mappings.NoElementException}.
 * </ul>
 */
public interface Mapping
    extends Mappings.FunctionMapping,
    Mappings.SourceMapping,
    Mappings.TargetMapping,
    Iterable<IntPair> {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns an iterator over the elements in this mapping.
   *
   * <p>This method is optional; implementations may throw
   * {@link UnsupportedOperationException}.
   */
  Iterator<IntPair> iterator();

  /**
   * Returns the number of sources. Valid sources will be in the range 0 ..
   * sourceCount.
   */
  int getSourceCount();

  /**
   * Returns the number of targets. Valid targets will be in the range 0 ..
   * targetCount.
   */
  int getTargetCount();

  MappingType getMappingType();

  /**
   * Returns whether this mapping is the identity.
   */
  boolean isIdentity();

  /**
   * Removes all elements in the mapping.
   */
  void clear();

  /**
   * Returns the number of elements in the mapping.
   */
  int size();
}

// End Mapping.java
