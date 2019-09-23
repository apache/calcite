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
package org.apache.calcite.sql.type;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class allows multiple existing {@link SqlOperandTypeChecker} rules to be
 * combined into one rule. For example, allowing an operand to be either string
 * or numeric could be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.OR,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>Similarly a rule that would only allow a numeric literal can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.AND,
 *         new SqlOperandTypeChecker[]{numericRule, literalRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>Finally, creating a signature expecting a string for the first operand and
 * a numeric for the second operand can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandsTypeChecking newCompositeRule =
 *     new CompositeOperandsTypeChecking(Composition.SEQUENCE,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>For SEQUENCE composition, the rules must be instances of
 * SqlSingleOperandTypeChecker, and signature generation is not supported. For
 * AND composition, only the first rule is used for signature generation.
 */
public class CompositeOperandTypeChecker implements SqlOperandTypeChecker {
  private final SqlOperandCountRange range;
  //~ Enums ------------------------------------------------------------------

  /** How operands are composed. */
  public enum Composition {
    AND, OR, SEQUENCE, REPEAT
  }

  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<? extends SqlOperandTypeChecker> allowedRules;
  protected final Composition composition;
  private final String allowedSignatures;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Use {@link OperandTypes#and},
   * {@link OperandTypes#or}.
   */
  CompositeOperandTypeChecker(
      Composition composition,
      ImmutableList<? extends SqlOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures,
      @Nullable SqlOperandCountRange range) {
    this.allowedRules = Objects.requireNonNull(allowedRules);
    this.composition = Objects.requireNonNull(composition);
    this.allowedSignatures = allowedSignatures;
    this.range = range;
    assert (range != null) == (composition == Composition.REPEAT);
    assert allowedRules.size() + (range == null ? 0 : 1) > 1;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean isOptional(int i) {
    for (SqlOperandTypeChecker allowedRule : allowedRules) {
      if (allowedRule.isOptional(i)) {
        return true;
      }
    }
    return false;
  }

  public ImmutableList<? extends SqlOperandTypeChecker> getRules() {
    return allowedRules;
  }

  public Consistency getConsistency() {
    return Consistency.NONE;
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    if (allowedSignatures != null) {
      return allowedSignatures;
    }
    if (composition == Composition.SEQUENCE) {
      throw new AssertionError(
          "specify allowedSignatures or override getAllowedSignatures");
    }
    StringBuilder ret = new StringBuilder();
    for (Ord<SqlOperandTypeChecker> ord
        : Ord.<SqlOperandTypeChecker>zip(allowedRules)) {
      if (ord.i > 0) {
        ret.append(SqlOperator.NL);
      }
      ret.append(ord.e.getAllowedSignatures(op, opName));
      if (composition == Composition.AND) {
        break;
      }
    }
    return ret.toString();
  }

  public SqlOperandCountRange getOperandCountRange() {
    switch (composition) {
    case REPEAT:
      return range;
    case SEQUENCE:
      return SqlOperandCountRanges.of(allowedRules.size());
    case AND:
    case OR:
    default:
      final List<SqlOperandCountRange> ranges =
          new AbstractList<SqlOperandCountRange>() {
            public SqlOperandCountRange get(int index) {
              return allowedRules.get(index).getOperandCountRange();
            }

            public int size() {
              return allowedRules.size();
            }
          };
      final int min = minMin(ranges);
      final int max = maxMax(ranges);
      SqlOperandCountRange composite =
          new SqlOperandCountRange() {
            public boolean isValidCount(int count) {
              switch (composition) {
              case AND:
                for (SqlOperandCountRange range : ranges) {
                  if (!range.isValidCount(count)) {
                    return false;
                  }
                }
                return true;
              case OR:
              default:
                for (SqlOperandCountRange range : ranges) {
                  if (range.isValidCount(count)) {
                    return true;
                  }
                }
                return false;
              }
            }

            public int getMin() {
              return min;
            }

            public int getMax() {
              return max;
            }
          };
      if (max >= 0) {
        for (int i = min; i <= max; i++) {
          if (!composite.isValidCount(i)) {
            // Composite is not a simple range. Can't simplify,
            // so return the composite.
            return composite;
          }
        }
      }
      return min == max
          ? SqlOperandCountRanges.of(min)
          : SqlOperandCountRanges.between(min, max);
    }
  }

  private int minMin(List<SqlOperandCountRange> ranges) {
    int min = Integer.MAX_VALUE;
    for (SqlOperandCountRange range : ranges) {
      min = Math.min(min, range.getMax());
    }
    return min;
  }

  private int maxMax(List<SqlOperandCountRange> ranges) {
    int max = Integer.MIN_VALUE;
    for (SqlOperandCountRange range : ranges) {
      if (range.getMax() < 0) {
        if (composition == Composition.OR) {
          return -1;
        }
      } else {
        max = Math.max(max, range.getMax());
      }
    }
    return max;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // 1. Check eagerly for binary arithmetic expressions.
    // 2. Check the comparability.
    // 3. Check if the operands have the right type.
    if (callBinding.getValidator().isTypeCoercionEnabled()) {
      final TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
      typeCoercion.binaryArithmeticCoercion(callBinding);
    }
    if (check(callBinding)) {
      return true;
    }
    if (!throwOnFailure) {
      return false;
    }
    if (composition == Composition.OR) {
      for (SqlOperandTypeChecker allowedRule : allowedRules) {
        allowedRule.checkOperandTypes(callBinding, true);
      }
    }

    // If no exception thrown, just throw a generic validation
    // signature error.
    throw callBinding.newValidationSignatureError();
  }

  private boolean check(SqlCallBinding callBinding) {
    switch (composition) {
    case REPEAT:
      if (!range.isValidCount(callBinding.getOperandCount())) {
        return false;
      }
      for (int operand : Util.range(callBinding.getOperandCount())) {
        for (SqlOperandTypeChecker rule : allowedRules) {
          if (!((SqlSingleOperandTypeChecker) rule).checkSingleOperandType(
              callBinding,
              callBinding.getCall().operand(operand),
              0,
              false)) {
            return false;
          }
        }
      }
      return true;

    case SEQUENCE:
      if (callBinding.getOperandCount() != allowedRules.size()) {
        return false;
      }
      for (Ord<SqlOperandTypeChecker> ord
          : Ord.<SqlOperandTypeChecker>zip(allowedRules)) {
        SqlOperandTypeChecker rule = ord.e;
        if (!((SqlSingleOperandTypeChecker) rule).checkSingleOperandType(
            callBinding,
            callBinding.getCall().operand(ord.i),
            0,
            false)) {
          if (callBinding.getValidator().isTypeCoercionEnabled()) {
            // Try type coercion for the call,
            // collect SqlTypeFamily and data type of all the operands.
            final List<SqlTypeFamily> families = allowedRules.stream()
                .filter(r -> r instanceof ImplicitCastOperandTypeChecker)
                .map(r -> ((ImplicitCastOperandTypeChecker) r).getOperandSqlTypeFamily(0))
                .collect(Collectors.toList());
            if (families.size() < allowedRules.size()) {
              // Not all the checkers are ImplicitCastOperandTypeChecker, returns early.
              return false;
            }
            final List<RelDataType> operandTypes = new ArrayList<>();
            for (int i = 0; i < callBinding.getOperandCount(); i++) {
              operandTypes.add(callBinding.getOperandType(i));
            }
            TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
            return typeCoercion.builtinFunctionCoercion(callBinding,
                operandTypes, families);
          }
          return false;
        }
      }
      return true;

    case AND:
      for (Ord<SqlOperandTypeChecker> ord
          : Ord.<SqlOperandTypeChecker>zip(allowedRules)) {
        SqlOperandTypeChecker rule = ord.e;
        if (!rule.checkOperandTypes(callBinding, false)) {
          // Avoid trying other rules in AND if the first one fails.
          return false;
        }
      }
      return true;

    case OR:
      // If there is an ImplicitCastOperandTypeChecker, check it without type coercion first,
      // if all check fails, try type coercion if it is allowed (default true).
      if (checkWithoutTypeCoercion(callBinding)) {
        return true;
      }
      for (Ord<SqlOperandTypeChecker> ord
          : Ord.<SqlOperandTypeChecker>zip(allowedRules)) {
        SqlOperandTypeChecker rule = ord.e;
        if (rule.checkOperandTypes(callBinding, false)) {
          return true;
        }
      }
      return false;

    default:
      throw new AssertionError();
    }
  }

  private boolean checkWithoutTypeCoercion(SqlCallBinding callBinding) {
    if (!callBinding.getValidator().isTypeCoercionEnabled()) {
      return false;
    }
    for (SqlOperandTypeChecker rule : allowedRules) {
      if (rule instanceof ImplicitCastOperandTypeChecker) {
        ImplicitCastOperandTypeChecker rule1 = (ImplicitCastOperandTypeChecker) rule;
        if (rule1.checkOperandTypesWithoutTypeCoercion(callBinding, false)) {
          return true;
        }
      }
    }
    return false;
  }
}

// End CompositeOperandTypeChecker.java
