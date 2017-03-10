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

import org.apache.calcite.rel.metadata.RelTableRef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

/**
 * Variable which references a field of an input relational expression
 */
public class RexTableInputRef extends RexInputRef {

  private final RelTableRef tableRef;

  public RexTableInputRef(RelTableRef tableRef, int index, RelDataType type) {
    super(index, type);
    this.tableRef = tableRef;
    this.digest = tableRef.toString() + ".$" + index;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RexTableInputRef
        && tableRef.equals(((RexTableInputRef) obj).tableRef)
        && index == ((RexTableInputRef) obj).index;
  }

  @Override public int hashCode() {
    return digest.hashCode();
  }

  public RelTableRef getTableRef() {
    return tableRef;
  }

  public String getQualifiedName() {
    return tableRef.getQualifiedName();
  }

  public int getIdentifier() {
    return tableRef.getIdentifier();
  }

  public static RexTableInputRef of(RelTableRef tableRef, int index, RelDataType type) {
    return new RexTableInputRef(tableRef, index, type);
  }

  public static RexTableInputRef of(RelTableRef tableRef, RexInputRef ref) {
    return new RexTableInputRef(tableRef, ref.getIndex(), ref.getType());
  }

  @Override public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitTableInputRef(this);
  }

  @Override public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitTableInputRef(this, arg);
  }

  @Override public SqlKind getKind() {
    return SqlKind.TABLE_INPUT_REF;
  }
}

// End RexTableInputRef.java
