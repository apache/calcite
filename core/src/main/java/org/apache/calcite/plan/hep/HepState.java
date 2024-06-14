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

/** Able to execute an instruction or program, and contains all mutable state
 * for that instruction.
 *
 * <p>The goal is that programs are re-entrant - they can be used by more than
 * one thread at a time. We achieve this by making instructions and programs
 * immutable. All mutable state is held in the state objects.
 *
 * <p>State objects are allocated, just before the program is executed, by
 * calling {@link HepInstruction#prepare(HepInstruction.PrepareContext)} on the
 * program and recursively on all of its instructions. */
abstract class HepState {
  final HepPlanner planner;
  final HepProgram.State programState;

  HepState(HepInstruction.PrepareContext px) {
    this.planner = px.planner;
    this.programState = px.programState;
  }

  /** Executes the instruction. */
  abstract void execute();

  /** Re-initializes the state. (The state was initialized when it was created
   * via {@link HepInstruction#prepare}.) */
  void init() {
  }
}
