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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Variable which references a column of a table occurrence in a relational plan.
 *
 * <p>This object is used by
 * {@link org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage}
 * and {@link org.apache.calcite.rel.metadata.BuiltInMetadata.AllPredicates}.
 *
 * <p>Given a relational expression, its purpose is to be able to reference uniquely
 * the provenance of a given expression. For that, it uses a unique table reference
 * (contained in a {@link RelTableRef}) and an column index within the table.
 *
 * <p>For example, {@code A.#0.$3 + 2} column {@code $3} in the {@code 0}
 * occurrence of table {@code A} in the plan.
 *
 * <p>Note that this kind of {@link RexNode} is an auxiliary data structure with
 * a very specific purpose and should not be used in relational expressions.
 */
public class RexTableInputRef extends RexInputRef {

  private final RelTableRef tableRef;

  /**
   * It's a tag, which ref is wrapped by out join. And it always emits nullable = true.
   * Input ref will be true, if it exists in right's input of left-join
   * or left's input of right-join are true
   */
  private final boolean forceNullable;

  private RexTableInputRef(RelTableRef tableRef, int index, RelDataType type,
      boolean forceNullable) {
    super(index, type);
    // Nullable of type should be true, because this input may emit nullable type,
    // if this input ref is wrapped by left/right join.
    assert !forceNullable || type.isNullable() : "Type should be nullable, if forced for nullable";
    this.tableRef = tableRef;
    this.forceNullable = forceNullable;
    this.digest = tableRef + ".$" + index + (forceNullable ? "(nullable)" : "");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof RexTableInputRef
        && tableRef.equals(((RexTableInputRef) obj).tableRef)
        && index == ((RexTableInputRef) obj).index
        && forceNullable == ((RexTableInputRef) obj).forceNullable;
  }

  @Override public int hashCode() {
    return Objects.hashCode(digest);
  }

  public RelTableRef getTableRef() {
    return tableRef;
  }

  public List<String> getQualifiedName() {
    return tableRef.getQualifiedName();
  }

  public int getIdentifier() {
    return tableRef.getEntityNumber();
  }

  public boolean getForceNullable() {
    return forceNullable;
  }

  public static RexTableInputRef of(RelTableRef tableRef, int index, RelDataType type) {
    return new RexTableInputRef(tableRef, index, type, false);
  }

  public static RexTableInputRef of(RelTableRef tableRef, RexInputRef ref) {
    return new RexTableInputRef(tableRef, ref.getIndex(), ref.getType(), false);
  }

  /**
   * Make a {@link RexTableInputRef} with join's nullable, rebuild type's nullable info,
   * if join's nullable is true.
   */
  public static RexTableInputRef of(RelDataTypeFactory factory, RelTableRef tableRef, int index,
      RelDataType type, boolean forceNullable) {
    if (forceNullable) {
      type = factory.createTypeWithNullability(type, true);
    }
    return new RexTableInputRef(tableRef, index, type, forceNullable);
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

  /** Identifies uniquely a table by its qualified name and its entity number
   * (occurrence). */
  public static class RelTableRef implements Comparable<RelTableRef> {

    private final RelOptTable table;
    private final int entityNumber;
    private final String digest;

    private RelTableRef(RelOptTable table, int entityNumber) {
      this.table = table;
      this.entityNumber = entityNumber;
      this.digest = table.getQualifiedName() + ".#" + entityNumber;
    }

    //~ Methods ----------------------------------------------------------------

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof RelTableRef
          && table.getQualifiedName().equals(((RelTableRef) obj).getQualifiedName())
          && entityNumber == ((RelTableRef) obj).entityNumber;
    }

    @Override public int hashCode() {
      return digest.hashCode();
    }

    public RelOptTable getTable() {
      return table;
    }

    public List<String> getQualifiedName() {
      return table.getQualifiedName();
    }

    public int getEntityNumber() {
      return entityNumber;
    }

    @Override public String toString() {
      return digest;
    }

    public static RelTableRef of(RelOptTable table, int entityNumber) {
      return new RelTableRef(table, entityNumber);
    }

    @Override public int compareTo(RelTableRef o) {
      return digest.compareTo(o.digest);
    }
  }
}
