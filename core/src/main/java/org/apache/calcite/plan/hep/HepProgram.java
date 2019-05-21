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
package org.apache.calcite.plan.hep;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * HepProgram specifies the order in which rules should be attempted by
 * {@link HepPlanner}. Use {@link HepProgramBuilder} to create a new
 * instance of HepProgram.
 *
 * <p>Note that the structure of a program is immutable, but the planner uses it
 * as read/write during planning, so a program can only be in use by a single
 * planner at a time.
 */
public class HepProgram {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Symbolic constant for matching until no more matches occur.
   */
  public static final int MATCH_UNTIL_FIXPOINT = Integer.MAX_VALUE;

  //~ Instance fields --------------------------------------------------------

  final ImmutableList<HepInstruction> instructions;

  int matchLimit;

  HepMatchOrder matchOrder;

  HepInstruction.EndGroup group;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new empty HepProgram. The program has an initial match order of
   * {@link org.apache.calcite.plan.hep.HepMatchOrder#DEPTH_FIRST}, and an initial
   * match limit of {@link #MATCH_UNTIL_FIXPOINT}.
   */
  HepProgram(List<HepInstruction> instructions) {
    this.instructions = ImmutableList.copyOf(instructions);
  }

  public static HepProgramBuilder builder() {
    return new HepProgramBuilder();
  }

  //~ Methods ----------------------------------------------------------------

  void initialize(boolean clearCache) {
    matchLimit = MATCH_UNTIL_FIXPOINT;
    matchOrder = HepMatchOrder.DEPTH_FIRST;
    group = null;

    for (HepInstruction instruction : instructions) {
      instruction.initialize(clearCache);
    }
  }
}

// End HepProgram.java
