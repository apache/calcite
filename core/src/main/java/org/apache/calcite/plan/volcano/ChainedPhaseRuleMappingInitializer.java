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
package org.apache.calcite.plan.volcano;

import java.util.Map;
import java.util.Set;

/**
 * ChainedPhaseRuleMappingInitializer is an abstract implementation of
 * {@link VolcanoPlannerPhaseRuleMappingInitializer} that allows additional
 * rules to be layered on top of those configured by a subordinate
 * {@link VolcanoPlannerPhaseRuleMappingInitializer}.
 *
 * @see VolcanoPlannerPhaseRuleMappingInitializer
 */
public abstract class ChainedPhaseRuleMappingInitializer
    implements VolcanoPlannerPhaseRuleMappingInitializer {
  //~ Instance fields --------------------------------------------------------

  private final VolcanoPlannerPhaseRuleMappingInitializer subordinate;

  //~ Constructors -----------------------------------------------------------

  public ChainedPhaseRuleMappingInitializer(
      VolcanoPlannerPhaseRuleMappingInitializer subordinate) {
    this.subordinate = subordinate;
  }

  //~ Methods ----------------------------------------------------------------

  public final void initialize(
      Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap) {
    // Initialize subordinate's mappings.
    subordinate.initialize(phaseRuleMap);

    // Initialize our mappings.
    chainedInitialize(phaseRuleMap);
  }

  /**
   * Extend this method to provide phase-to-rule mappings beyond what is
   * provided by this initializer's subordinate.
   *
   * <p>When this method is called, the map will already be pre-initialized
   * with empty sets for each VolcanoPlannerPhase. Implementations must not
   * return having added or removed keys from the map, although it is safe to
   * temporarily add or remove keys.
   *
   * @param phaseRuleMap the {@link VolcanoPlannerPhase}-rule description map
   * @see VolcanoPlannerPhaseRuleMappingInitializer
   */
  public abstract void chainedInitialize(
      Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap);
}

// End ChainedPhaseRuleMappingInitializer.java
