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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.rex.RexProgram;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Calc}. */
public class MutableCalc extends MutableSingleRel {
  public final RexProgram program;

  private MutableCalc(MutableRel input, RexProgram program) {
    super(MutableRelType.CALC, program.getOutputRowType(), input);
    this.program = program;
  }

  /**
   * Creates a MutableCalc
   *
   * @param input   Input relational expression
   * @param program Calc program
   */
  public static MutableCalc of(MutableRel input, RexProgram program) {
    return new MutableCalc(input, program);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableCalc
        && MutableRel.STRING_EQUIVALENCE.equivalent(
            program, ((MutableCalc) obj).program)
        && input.equals(((MutableCalc) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input,
        MutableRel.STRING_EQUIVALENCE.hash(program));
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Calc(program: ").append(program).append(")");
  }

  @Override public MutableRel clone() {
    return MutableCalc.of(input.clone(), program);
  }
}

// End MutableCalc.java
