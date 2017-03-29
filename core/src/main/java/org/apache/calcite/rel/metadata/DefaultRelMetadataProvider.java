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

import com.google.common.collect.ImmutableList;

/**
 * DefaultRelMetadataProvider supplies a default implementation of the
 * {@link RelMetadataProvider} interface. It provides generic formulas and
 * derivation rules for the standard logical algebra; coverage corresponds to
 * the methods declared in {@link RelMetadataQuery}.
 */
public class DefaultRelMetadataProvider extends ChainedRelMetadataProvider {
  public static final DefaultRelMetadataProvider INSTANCE =
      new DefaultRelMetadataProvider();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new default provider. This provider defines "catch-all"
   * handlers for generic RelNodes, so it should always be given lowest
   * priority when chaining.
   *
   * <p>Use this constructor only from a sub-class. Otherwise use the singleton
   * instance, {@link #INSTANCE}.
   */
  protected DefaultRelMetadataProvider() {
    super(
        ImmutableList.of(
            RelMdPercentageOriginalRows.SOURCE,
            RelMdColumnOrigins.SOURCE,
            RelMdExpressionLineage.SOURCE,
            RelMdTableReferences.SOURCE,
            RelMdNodeTypes.SOURCE,
            RelMdRowCount.SOURCE,
            RelMdMaxRowCount.SOURCE,
            RelMdMinRowCount.SOURCE,
            RelMdUniqueKeys.SOURCE,
            RelMdColumnUniqueness.SOURCE,
            RelMdPopulationSize.SOURCE,
            RelMdSize.SOURCE,
            RelMdParallelism.SOURCE,
            RelMdDistribution.SOURCE,
            RelMdMemory.SOURCE,
            RelMdDistinctRowCount.SOURCE,
            RelMdSelectivity.SOURCE,
            RelMdExplainVisibility.SOURCE,
            RelMdPredicates.SOURCE,
            RelMdAllPredicates.SOURCE,
            RelMdCollation.SOURCE));
  }
}

// End DefaultRelMetadataProvider.java
