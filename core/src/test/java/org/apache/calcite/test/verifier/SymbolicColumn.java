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
package org.apache.calcite.test.verifier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;
import com.microsoft.z3.Expr;
import com.microsoft.z3.Sort;


/**
 * A Symbolic column symbolically represent a column.
 * It has two field: SymbolicValue, and SymbolicNull
 * SymbolicValue represents the value of column
 * SymbolicNull indicates if the value is null
 **/

public class SymbolicColumn {
  /** Each SymbolicColumn is unique **/
  private static int count = 0;

  private Expr symbolicValue;
  private BoolExpr symbolicNull;

  public static SymbolicColumn mkNewSymbolicColumn(Context z3Context, RexNode node) {
    return mkNewSymbolicColumn(z3Context, node.getType());
  }

  public static SymbolicColumn mkNewSymbolicColumn(Context z3Context, RelDataType type) {
    Expr value = z3Context.mkConst("value" + count, type2Sort(type, z3Context));
    BoolExpr valueNull = z3Context.mkBoolConst("isN" + count);
    count++;
    return new SymbolicColumn(value, valueNull);
  }

  private static Sort type2Sort(RelDataType type, Context z3Context) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (SqlTypeName.INT_TYPES.contains(typeName)) {
      return z3Context.mkIntSort();
    } else if (SqlTypeName.APPROX_TYPES.contains(type)) {
      return z3Context.mkRealSort();
    } else if (SqlTypeName.BOOLEAN_TYPES.contains(type)) {
      return z3Context.mkBoolSort();
    } else {
      /** unhandled type for now **/
      return null;
    }
  }

  public static Expr dummyValue(RexNode node, Context z3Context) {
    RelDataType type = node.getType();
    Expr dummyValue = z3Context.mkConst("value" + count, type2Sort(type, z3Context));
    count++;
    return dummyValue;
  }

  public SymbolicColumn(Expr symbolicValue, BoolExpr symbolicNull) {
    this.symbolicValue = symbolicValue;
    this.symbolicNull = symbolicNull;
  }

  public Expr getSymbolicValue() {
    return this.symbolicValue;
  }

  public BoolExpr getSymbolicNull() {
    return this.symbolicNull;
  }

  @Override public String toString() {
    return "(Symbolic Value:" + this.symbolicValue + " , Is Null:" + this.symbolicNull + ")";
  }
}
