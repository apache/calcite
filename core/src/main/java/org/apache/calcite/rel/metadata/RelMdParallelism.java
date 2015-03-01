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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Default implementations of the
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism}
 * metadata provider for the standard logical algebra.
 *
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery#isPhaseTransition
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery#splitCount
 */
public class RelMdParallelism {
  /** Source for
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism}. */
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new RelMdParallelism(),
          BuiltInMethod.IS_PHASE_TRANSITION.method,
          BuiltInMethod.SPLIT_COUNT.method);

  //~ Constructors -----------------------------------------------------------

  protected RelMdParallelism() {}

  //~ Methods ----------------------------------------------------------------

  /** Catch-all implementation for
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism#isPhaseTransition()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#isPhaseTransition
   */
  public Boolean isPhaseTransition(RelNode rel) {
    return false;
  }

  public Boolean isPhaseTransition(TableScan rel) {
    return true;
  }

  public Boolean isPhaseTransition(Values rel) {
    return true;
  }

  public Boolean isPhaseTransition(Exchange rel) {
    return true;
  }

  /** Catch-all implementation for
   * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism#splitCount()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#splitCount
   */
  public Integer splitCount(RelNode rel) {
    return 1;
  }
}

// End RelMdParallelism.java
