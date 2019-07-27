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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Definition of the <code>MIN</code> and <code>MAX</code> aggregate functions,
 * returning the returns the smallest/largest of the values which go into it.
 *
 * <p>There are 3 forms:
 *
 * <dl>
 * <dt>sum(<em>primitive type</em>)
 * <dd>values are compared using '&lt;'
 *
 * <dt>sum({@link java.lang.Comparable})
 * <dd>values are compared using {@link java.lang.Comparable#compareTo}
 *
 * <dt>sum({@link java.util.Comparator}, {@link java.lang.Object})
 * <dd>the {@link java.util.Comparator#compare} method of the comparator is used
 * to compare pairs of objects. The comparator is a startup argument, and must
 * therefore be constant for the duration of the aggregation.
 * </dl>
 */
public class SqlMinMaxAggFunction extends SqlAggFunction {
  //~ Static fields/initializers ---------------------------------------------

  public static final int MINMAX_INVALID = -1;
  public static final int MINMAX_PRIMITIVE = 0;
  public static final int MINMAX_COMPARABLE = 1;
  public static final int MINMAX_COMPARATOR = 2;

  //~ Instance fields --------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public final List<RelDataType> argTypes;
  private final int minMaxKind;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlMinMaxAggFunction. */
  public SqlMinMaxAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.COMPARABLE_ORDERED,
        SqlFunctionCategory.SYSTEM,
        false,
        false,
        Optionality.FORBIDDEN);
    this.argTypes = ImmutableList.of();
    this.minMaxKind = MINMAX_COMPARABLE;
    Preconditions.checkArgument(kind == SqlKind.MIN
        || kind == SqlKind.MAX);
  }

  @Deprecated // to be removed before 2.0
  public SqlMinMaxAggFunction(
      List<RelDataType> argTypes,
      boolean isMin,
      int minMaxKind) {
    this(isMin ? SqlKind.MIN : SqlKind.MAX);
    assert argTypes.isEmpty();
    assert minMaxKind == MINMAX_COMPARABLE;
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public boolean isMin() {
    return kind == SqlKind.MIN;
  }

  @Deprecated // to be removed before 2.0
  public int getMinMaxKind() {
    return minMaxKind;
  }

  @Override public Optionality getDistinctOptionality() {
    return Optionality.IGNORED;
  }

  @SuppressWarnings("deprecation")
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    switch (minMaxKind) {
    case MINMAX_PRIMITIVE:
    case MINMAX_COMPARABLE:
      return argTypes;
    case MINMAX_COMPARATOR:
      return argTypes.subList(1, 2);
    default:
      throw new AssertionError("bad kind: " + minMaxKind);
    }
  }

  @SuppressWarnings("deprecation")
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    switch (minMaxKind) {
    case MINMAX_PRIMITIVE:
    case MINMAX_COMPARABLE:
      return argTypes.get(0);
    case MINMAX_COMPARATOR:
      return argTypes.get(1);
    default:
      throw new AssertionError("bad kind: " + minMaxKind);
    }
  }

  @Override public <T> T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }
}

// End SqlMinMaxAggFunction.java
