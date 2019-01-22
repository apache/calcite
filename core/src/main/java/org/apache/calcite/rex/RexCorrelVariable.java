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
package org.apache.calcite.rex;

import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;

/**
 * Reference to the current row of a correlating relational expression.
 *
 * <p>Correlating variables are introduced when performing nested loop joins.
 * Each row is received from one side of the join, a correlating variable is
 * assigned a value, and the other side of the join is restarted.</p>
 */
public class RexCorrelVariable extends RexVariable {
  public final CorrelationId id;

  //~ Constructors -----------------------------------------------------------

  RexCorrelVariable(
      CorrelationId id,
      RelDataType type) {
    super(id.getName(), type);
    this.id = Objects.requireNonNull(id);
  }

  //~ Methods ----------------------------------------------------------------

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitCorrelVariable(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitCorrelVariable(this, arg);
  }

  @Override public SqlKind getKind() {
    return SqlKind.CORREL_VARIABLE;
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RexCorrelVariable
        && digest.equals(((RexCorrelVariable) obj).digest)
        && type.equals(((RexCorrelVariable) obj).type)
        && id.equals(((RexCorrelVariable) obj).id);
  }

  @Override public int hashCode() {
    return Objects.hash(digest, type, id);
  }
}

// End RexCorrelVariable.java
