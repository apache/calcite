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
package org.apache.calcite.materialize;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Util;

import java.util.Objects;
import javax.annotation.Nonnull;

/** Table registered in the graph. */
public class LatticeTable {
  @Nonnull public final RelOptTable t;
  @Nonnull public final String alias;

  LatticeTable(RelOptTable table) {
    t = Objects.requireNonNull(table);
    alias = Objects.requireNonNull(Util.last(table.getQualifiedName()));
  }

  @Override public int hashCode() {
    return t.getQualifiedName().hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof LatticeTable
        && t.getQualifiedName().equals(
            ((LatticeTable) obj).t.getQualifiedName());
  }

  @Override public String toString() {
    return t.getQualifiedName().toString();
  }

  RelDataTypeField field(int i) {
    return t.getRowType().getFieldList().get(i);
  }
}

// End LatticeTable.java
