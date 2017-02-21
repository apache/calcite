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

import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;
import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.Window}. */
public class MutableWindow extends MutableSingleRel {
  public final List<Group> groups;
  public final List<RexLiteral> constants;

  private MutableWindow(RelDataType rowType, MutableRel input,
      List<Group> groups, List<RexLiteral> constants) {
    super(MutableRelType.WINDOW, rowType, input);
    this.groups = groups;
    this.constants = constants;
  }

  /**
   * Creates a MutableWindow.
   *
   * @param rowType   Row type
   * @param input     Input relational expression
   * @param groups    Window groups
   * @param constants List of constants that are additional inputs
   */
  public static MutableWindow of(RelDataType rowType,
      MutableRel input, List<Group> groups, List<RexLiteral> constants) {
    return new MutableWindow(rowType, input, groups, constants);
  }

  @Override public boolean equals(Object obj) {
    return obj == this
        || obj instanceof MutableWindow
        && groups.equals(((MutableWindow) obj).groups)
        && constants.equals(((MutableWindow) obj).constants)
        && input.equals(((MutableWindow) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, groups, constants);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("Window(groups: ").append(groups)
        .append(", constants: ").append(constants).append(")");
  }

  @Override public MutableRel clone() {
    return MutableWindow.of(rowType, input.clone(), groups, constants);
  }
}

// End MutableWindow.java
