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
package org.eigenbase.rel;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

/**
 * Context from which a relational expression can initialize itself,
 * reading from a serialized form of the relational expression.
 */
public interface RelInput {
  RelOptCluster getCluster();

  RelTraitSet getTraitSet();

  RelOptTable getTable(String table);

  /**
   * Returns the input relational expression. Throws if there is not precisely
   * one input.
   */
  RelNode getInput();

  List<RelNode> getInputs();

  /**
   * Returns an expression.
   */
  RexNode getExpression(String tag);

  BitSet getBitSet(String tag);

  List<AggregateCall> getAggregateCalls(String tag);

  Object get(String tag);

  /**
   * Returns a {@code float} value. Throws if wrong type.
   */
  String getString(String tag);

  /**
   * Returns a {@code float} value. Throws if not present or wrong type.
   */
  float getFloat(String tag);

  /**
   * Returns an enum value. Throws if not a valid member.
   */
  <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass);

  List<RexNode> getExpressionList(String tag);

  RelDataType getRowType(String tag);

  RelDataType getRowType(String expressionsTag, String fieldsTag);

  RelCollation getCollation();

  List<List<RexLiteral>> getTuples(String tag);

  boolean getBoolean(String tag);
}

// End RelInput.java
