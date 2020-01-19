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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Variable which references a parameter in lambda expression.
 *
 * <p>Fields of the input are 0-based. If there is more than one input, they are
 * numbered consecutively. For example</p>
 *
 * (a,b)->{expression}
 *
 * <p>then the fields are:</p>
 *
 * <ul>
 * <li>Field #0: a</li>
 * <li>Field #1: b</li>
 * </ul>
 *
 * <p>So <code>RexLambdaRef(0, Integer)</code> is the correct reference for the
 * field a.</p>
 */
public class RexLambdaRef extends RexSlot {
  //~ Static fields/initializers ---------------------------------------------

  // list of common names, to reduce memory allocations
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private static final List<String> NAMES = new SelfPopulatingList("$l", 30);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an lambda parameter variable.
   *
   * @param index Index of the lambda parameter in the lambda expression
   * @param type Type of the column
   */
  public RexLambdaRef(int index, RelDataType type) {
    super(createName(index), index, type);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RexLambdaRef
        && index == ((RexLambdaRef) obj).index;
  }

  @Override public int hashCode() {
    return index;
  }

  /**
   * Creates a reference to a given field in a row type.
   */
  public static RexLambdaRef of(int index, RelDataType rowType) {
    return of(index, rowType.getFieldList());
  }

  /**
   * Creates a reference to a given field in a list of fields.
   */
  public static RexLambdaRef of(int index, List<RelDataTypeField> fields) {
    return new RexLambdaRef(index, fields.get(index).getType());
  }

  /**
   * Creates a reference to a given field in a list of fields.
   */
  public static Pair<RexNode, String> of2(
      int index,
      List<RelDataTypeField> fields) {
    final RelDataTypeField field = fields.get(index);
    return Pair.of(
        new RexLambdaRef(index, field.getType()),
        field.getName());
  }

  @Override public SqlKind getKind() {
    return SqlKind.LAMBDA_REF;
  }

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitLambdaRef(this);
  }

  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return visitor.visitLambdaRef(this, arg);
  }

  /**
   * Creates a name for an lambda paramter reference, of the form "$lindex". If the index is low,
   * uses a cache of common names, to reduce gc.
   */
  public static String createName(int index) {
    return NAMES.get(index);
  }
}
