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
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class allows multiple existing {@link SqlOperandTypeChecker} rules to be
 * combined into one rule. For example, allowing an operand to be either string
 * or numeric could be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandTypeChecker newCompositeRule =
 *     new CompositeOperandTypeChecker(Composition.OR,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>Similarly a rule that would only allow a numeric literal can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandTypeChecker newCompositeRule =
 *     new CompositeOperandTypeChecker(Composition.AND,
 *         new SqlOperandTypeChecker[]{numericRule, literalRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>Finally, creating a signature expecting a string for the first operand and
 * a numeric for the second operand can be done by:
 *
 * <blockquote>
 * <pre><code>
 * CompositeOperandTypeChecker newCompositeRule =
 *     new CompositeOperandTypeChecker(Composition.SEQUENCE,
 *         new SqlOperandTypeChecker[]{stringRule, numericRule});
 * </code></pre>
 * </blockquote>
 *
 * <p>For SEQUENCE composition, the rules must be instances of
 * SqlSingleOperandTypeChecker, and signature generation is not supported. For
 * AND composition, only the first rule is used for signature generation.
 */
public class CompositeOperandTypeChecker implements SqlOperandTypeChecker {
  private final @Nullable SqlOperandCountRange range;
  //~ Enums ------------------------------------------------------------------

  /** How operands are composed. */
  public enum Composition {
    AND, OR, SEQUENCE, REPEAT
  }

  //~ Instance fields --------------------------------------------------------

  // It is not clear if @UnknownKeyFor is needed here or not, however, checkerframework inference
  // fails otherwise, see https://github.com/typetools/checker-framework/issues/4048
  protected final ImmutableList<@UnknownKeyFor ? extends SqlOperandTypeChecker> allowedRules;
  protected final Composition composition;
  private final @Nullable String allowedSignatures;
  private final @Nullable BiFunction<SqlOperator, String, String> signatureGenerator;

  //~ Constructors -----------------------------------------------------------

  /** Creates a CompositeOperandTypeChecker. Outside this package, use
   * {@link SqlOperandTypeChecker#and(SqlOperandTypeChecker)},
   * {@link OperandTypes#and}, {@link OperandTypes#or} and similar. */
  protected CompositeOperandTypeChecker(Composition composition,
      ImmutableList<? extends SqlOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures,
      @Nullable BiFunction<SqlOperator, String, String> signatureGenerator,
      @Nullable SqlOperandCountRange range) {
    this.allowedRules = requireNonNull(allowedRules, "allowedRules");
    this.composition = requireNonNull(composition, "composition");
    this.allowedSignatures = allowedSignatures;
    this.signatureGenerator = signatureGenerator;
    this.range = range;
    assert (range != null) == (composition == Composition.REPEAT);
    assert allowedRules.size() + (range == null ? 0 : 1) > 1;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public CompositeOperandTypeChecker withGenerator(
      BiFunction<SqlOperator, String, String> signatureGenerator) {
    return this.signatureGenerator == signatureGenerator
        ? this
        : new CompositeOperandTypeChecker(composition, allowedRules,
            allowedSignatures, signatureGenerator, range);
  }

  @Override public boolean isOptional(int i) {
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

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    if (allowedSignatures != null) {
      return allowedSignatures;
    }
    if (signatureGenerator != null) {
      return signatureGenerator.apply(op, opName);
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

  @Override public SqlOperandCountRange getOperandCountRange() {
    switch (composition) {
    case REPEAT:
      return requireNonNull(range, "range");
    case SEQUENCE:
      return SqlOperandCountRanges.of(allowedRules.size());
    case AND:
    case OR:
    default:
      final List<SqlOperandCountRange> ranges =
          new AbstractList<SqlOperandCountRange>() {
            @Override public SqlOperandCountRange get(int index) {
              return allowedRules.get(index).getOperandCountRange();
            }

            @Override public int size() {
              return allowedRules.size();
            }
          };
      final int min = minMin(ranges);
      final int max = maxMax(ranges);
      SqlOperandCountRange composite =
          new SqlOperandCountRange() {
            @Override public boolean isValidCount(int count) {
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

            @Override public int getMin() {
              return min;
            }

            @Override public int getMax() {
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

  private static int minMin(List<SqlOperandCountRange> ranges) {
    int min = Integer.MAX_VALUE;
    for (SqlOperandCountRange range : ranges) {
      min = Math.min(min, range.getMin());
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

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // 1. Check eagerly for binary arithmetic expressions.
    // 2. Check the comparability.
    // 3. Check if the operands have the right type.
    if (callBinding.isTypeCoercionEnabled()) {
      final TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
      typeCoercion.binaryArithmeticCoercion(callBinding);
    }
    if (check(callBinding, false)) {
      return true;
    }
    if (!throwOnFailure) {
      return false;
    }
    // Check again, to cause error to be thrown.
    switch (composition) {
    default:
      break;
    case OR:
    case SEQUENCE:
      check(callBinding, true);
    }

    // If no exception thrown, just throw a generic validation
    // signature error.
    throw callBinding.newValidationSignatureError();
  }

  private boolean check(SqlCallBinding callBinding, boolean throwOnFailure) {
    switch (composition) {
    case REPEAT:
      if (!requireNonNull(range, "range").isValidCount(callBinding.getOperandCount())) {
        return false;
      }
      for (int operand : Util.range(callBinding.getOperandCount())) {
        for (SqlOperandTypeChecker rule : allowedRules) {
          if (!((SqlSingleOperandTypeChecker) rule).checkSingleOperandType(
              callBinding,
              callBinding.getCall().operand(operand),
              0,
              throwOnFailure)) {
            if (callBinding.isTypeCoercionEnabled()) {
              return coerceOperands(callBinding, true);
            }
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
            ord.i,
            throwOnFailure)) {
          if (callBinding.isTypeCoercionEnabled()) {
            return coerceOperands(callBinding, false);
          }
          return false;
        }
      }
      return true;

    case AND:
      for (Ord<SqlOperandTypeChecker> ord
          : Ord.<SqlOperandTypeChecker>zip(allowedRules)) {
        SqlOperandTypeChecker rule = ord.e;
        if (!rule.checkOperandTypes(callBinding, throwOnFailure)) {
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
        if (rule.checkOperandTypes(callBinding, throwOnFailure)) {
          return true;
        }
      }
      return false;

    default:
      throw new AssertionError();
    }
  }

  /** Tries to coerce the operands based on the defined type families. */
  private boolean coerceOperands(SqlCallBinding callBinding, boolean repeat) {
    // Type coercion for the call,
    // collect SqlTypeFamily and data type of all the operands.
    List<SqlTypeFamily> families = allowedRules.stream()
        .filter(r -> r instanceof ImplicitCastOperandTypeChecker)
        // All the rules are SqlSingleOperandTypeChecker.
        .map(r -> ((ImplicitCastOperandTypeChecker) r).getOperandSqlTypeFamily(0))
        .collect(Collectors.toList());
    if (families.size() < allowedRules.size()) {
      // Not all the checkers are ImplicitCastOperandTypeChecker, returns early.
      return false;
    }
    if (repeat) {
      assert families.size() == 1;
      families = Collections.nCopies(callBinding.getOperandCount(), families.get(0));
    }
    final List<RelDataType> operandTypes = new ArrayList<>();
    for (int i = 0; i < callBinding.getOperandCount(); i++) {
      operandTypes.add(callBinding.getOperandType(i));
    }
    TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
    return typeCoercion.builtinFunctionCoercion(callBinding,
        operandTypes, families);
  }

  private boolean checkWithoutTypeCoercion(SqlCallBinding callBinding) {
    if (!callBinding.isTypeCoercionEnabled()) {
      return false;
    }
    SqlCallBinding sqlBindingWithoutTypeCoercion =
        new SqlCallBinding(callBinding.getValidator(), callBinding.getScope(),
            callBinding.getCall()) {
          @Override public boolean isTypeCoercionEnabled() {
            return false;
          }
        };
    for (SqlOperandTypeChecker rule : allowedRules) {
      if (rule.checkOperandTypes(sqlBindingWithoutTypeCoercion, false)) {
        return true;
      }
      if (rule instanceof ImplicitCastOperandTypeChecker) {
        ImplicitCastOperandTypeChecker rule1 = (ImplicitCastOperandTypeChecker) rule;
        if (rule1.checkOperandTypesWithoutTypeCoercion(callBinding, false)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override public @Nullable SqlOperandTypeInference typeInference() {
    if (composition == Composition.REPEAT) {
      SqlOperandTypeChecker checker = Iterables.getOnlyElement(allowedRules);
      if (checker instanceof SqlOperandTypeInference) {
        final SqlOperandTypeInference rule =
            (SqlOperandTypeInference) checker;
        return (callBinding, returnType, operandTypes) -> {
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            final RelDataType[] operandTypes0 = new RelDataType[1];
            rule.inferOperandTypes(callBinding, returnType, operandTypes0);
            operandTypes[i] = operandTypes0[0];
          }
        };
      }
    }
    return null;
  }
}
