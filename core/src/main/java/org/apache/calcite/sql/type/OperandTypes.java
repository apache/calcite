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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Strategies for checking operand types.
 *
 * <p>This class defines singleton instances of strategy objects for operand
 * type-checking. {@link org.apache.calcite.sql.type.ReturnTypes}
 * and {@link org.apache.calcite.sql.type.InferTypes} provide similar strategies
 * for operand type inference and operator return type inference.
 *
 * <p>Note to developers: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 *
 * @see org.apache.calcite.sql.type.SqlOperandTypeChecker
 * @see org.apache.calcite.sql.type.ReturnTypes
 * @see org.apache.calcite.sql.type.InferTypes
 */
public abstract class OperandTypes {
  private OperandTypes() {
  }

  /**
   * Creates a checker that passes if each operand is a member of a
   * corresponding family.
   */
  public static FamilyOperandTypeChecker family(SqlTypeFamily... families) {
    return new FamilyOperandTypeChecker(ImmutableList.copyOf(families),
        i -> false);
  }

  /**
   * Creates a checker that passes if each operand is a member of a
   * corresponding family, and allows specified parameters to be optional.
   */
  public static FamilyOperandTypeChecker family(List<SqlTypeFamily> families,
      Predicate<Integer> optional) {
    return new FamilyOperandTypeChecker(families, optional);
  }

  /**
   * Creates a checker that passes if each operand is a member of a
   * corresponding family.
   */
  public static FamilyOperandTypeChecker family(List<SqlTypeFamily> families) {
    return family(families, i -> false);
  }

  /**
   * Creates a checker that passes if the operand is an interval appropriate for
   * a given date/time type. For example, the time frame HOUR is appropriate for
   * type TIMESTAMP or DATE but not TIME.
   */
  public static SqlSingleOperandTypeChecker interval(
      Iterable<TimeUnitRange> ranges) {
    return new IntervalOperandTypeChecker(ImmutableSet.copyOf(ranges));
  }

  /**
   * Creates a checker for user-defined functions (including user-defined
   * aggregate functions, table functions, and table macros).
   *
   * <p>Unlike built-in functions, there is a fixed number of parameters,
   * and the parameters have names. You can ask for the type of a parameter
   * without providing a particular call (and with it actual arguments) but you
   * do need to provide a type factory, and therefore the types are only good
   * for the duration of the current statement.
   */
  public static SqlOperandMetadata operandMetadata(List<SqlTypeFamily> families,
      Function<RelDataTypeFactory, List<RelDataType>> typesFactory,
      IntFunction<String> operandName, Predicate<Integer> optional) {
    return new OperandMetadataImpl(families, typesFactory, operandName,
        optional);
  }

  /**
   * Creates a checker that passes if any one of the rules passes.
   */
  public static SqlOperandTypeChecker or(SqlOperandTypeChecker... rules) {
    return composite(CompositeOperandTypeChecker.Composition.OR,
        ImmutableList.copyOf(rules), null, null, null);
  }

  /**
   * Creates a checker that passes if all of the rules pass.
   */
  public static SqlOperandTypeChecker and(SqlOperandTypeChecker... rules) {
    return and_(ImmutableList.copyOf(rules));
  }

  private static SqlOperandTypeChecker and_(
      Iterable<SqlOperandTypeChecker> rules) {
    return composite(CompositeOperandTypeChecker.Composition.AND,
        ImmutableList.copyOf(rules), null, null, null);
  }

  /**
   * Creates a single-operand checker that passes if any one of the rules
   * passes.
   */
  public static SqlSingleOperandTypeChecker or(
      SqlSingleOperandTypeChecker... rules) {
    return compositeSingle(CompositeOperandTypeChecker.Composition.OR,
        ImmutableList.copyOf(rules), null);
  }

  /**
   * Creates a single-operand checker that passes if all of the rules
   * pass.
   */
  public static SqlSingleOperandTypeChecker and(
      SqlSingleOperandTypeChecker... rules) {
    return compositeSingle(CompositeOperandTypeChecker.Composition.AND,
        ImmutableList.copyOf(rules), null);
  }

  /** Creates a CompositeSingleOperandTypeChecker. Outside this package, use
   * {@link SqlSingleOperandTypeChecker#and(SqlSingleOperandTypeChecker)},
   * {@link OperandTypes#and}, {@link OperandTypes#or} and similar. */
  private static SqlSingleOperandTypeChecker compositeSingle(
      CompositeOperandTypeChecker.Composition composition,
      List<? extends SqlSingleOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures) {
    final List<SqlSingleOperandTypeChecker> list = new ArrayList<>(allowedRules);
    switch (composition) {
    default:
      break;
    case AND:
    case OR:
      flatten(list, c -> c instanceof CompositeSingleOperandTypeChecker
          && ((CompositeSingleOperandTypeChecker) c).composition == composition
          ? ((CompositeSingleOperandTypeChecker) c).getRules()
          : null);
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new CompositeSingleOperandTypeChecker(composition,
        ImmutableList.copyOf(list), allowedSignatures);
  }

  /**
   * Creates an operand checker from a sequence of single-operand checkers.
   */
  public static SqlOperandTypeChecker sequence(String allowedSignatures,
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.SEQUENCE,
        ImmutableList.copyOf(rules), allowedSignatures, null, null);
  }

  /**
   * Creates a checker that passes if all of the rules pass for each operand,
   * using a given operand count strategy.
   */
  public static SqlOperandTypeChecker repeat(SqlOperandCountRange range,
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.REPEAT,
        ImmutableList.copyOf(rules), null, null, range);
  }

  /**
   * Creates an operand checker that applies a single-operand checker to
   * the {@code ordinal}th operand.
   */
  public static SqlOperandTypeChecker nth(int ordinal, int operandCount,
      SqlSingleOperandTypeChecker rule) {
    SqlSingleOperandTypeChecker[] rules =
        new SqlSingleOperandTypeChecker[operandCount];
    Arrays.fill(rules, ANY);
    rules[ordinal] = rule;
    return sequence("", rules);
  }

  /** Creates a CompositeOperandTypeChecker. Outside this package, use
   * {@link SqlSingleOperandTypeChecker#and(SqlSingleOperandTypeChecker)},
   * {@link OperandTypes#and}, {@link OperandTypes#or} and similar. */
  static SqlOperandTypeChecker composite(
      CompositeOperandTypeChecker.Composition composition,
      List<? extends SqlOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures,
      @Nullable BiFunction<SqlOperator, String, String> signatureGenerator,
      @Nullable SqlOperandCountRange range) {
    final List<SqlOperandTypeChecker> list = new ArrayList<>(allowedRules);
    switch (composition) {
    default:
      break;
    case AND:
    case OR:
      flatten(list, c -> c instanceof CompositeOperandTypeChecker
          && ((CompositeOperandTypeChecker) c).composition == composition
          ? ((CompositeOperandTypeChecker) c).getRules()
          : null);
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new CompositeOperandTypeChecker(composition,
        ImmutableList.copyOf(list), allowedSignatures, signatureGenerator, range);
  }

  /** Helper for {@link #compositeSingle} and {@link #composite}. */
  private static <E> void flatten(List<E> list,
      Function<E, @Nullable List<? extends E>> expander) {
    for (int i  = 0; i < list.size();) {
      @Nullable List<? extends E> list2 = expander.apply(list.get(i));
      if (list2 == null) {
        ++i;
      } else {
        list.remove(i);
        list.addAll(i, list2);
      }
    }
  }

  // ----------------------------------------------------------------------
  // SqlOperandTypeChecker definitions
  // ----------------------------------------------------------------------

  //~ Static fields/initializers ---------------------------------------------

  /**
   * Operand type-checking strategy for an operator which takes no operands.
   */
  public static final SqlSingleOperandTypeChecker NILADIC = family();

  /**
   * Operand type-checking strategy for an operator with no restrictions on
   * number or type of operands.
   */
  public static final SqlOperandTypeChecker VARIADIC =
      variadic(SqlOperandCountRanges.any());

  /** Operand type-checking strategy that allows one or more operands. */
  public static final SqlOperandTypeChecker ONE_OR_MORE =
      variadic(SqlOperandCountRanges.from(1));

  public static SqlOperandTypeChecker variadic(
      final SqlOperandCountRange range) {
    return new SqlOperandTypeChecker() {
      @Override public boolean checkOperandTypes(
          SqlCallBinding callBinding,
          boolean throwOnFailure) {
        return range.isValidCount(callBinding.getOperandCount());
      }

      @Override public SqlOperandCountRange getOperandCountRange() {
        return range;
      }

      @Override public String getAllowedSignatures(SqlOperator op, String opName) {
        return opName + "(...)";
      }
    };
  }

  public static final SqlSingleOperandTypeChecker BOOLEAN =
      family(SqlTypeFamily.BOOLEAN);

  public static final SqlSingleOperandTypeChecker BOOLEAN_BOOLEAN =
      family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN);

  public static final SqlSingleOperandTypeChecker NUMERIC =
      family(SqlTypeFamily.NUMERIC);

  public static final SqlSingleOperandTypeChecker INTEGER =
      family(SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker NUMERIC_OPTIONAL_INTEGER =
      family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER),
          // Second operand optional (operand index 0, 1)
          number -> number == 1);

  public static final SqlSingleOperandTypeChecker NUMERIC_INTEGER =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker NUMERIC_NUMERIC =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);

  public static final SqlSingleOperandTypeChecker EXACT_NUMERIC =
      family(SqlTypeFamily.EXACT_NUMERIC);

  public static final SqlSingleOperandTypeChecker EXACT_NUMERIC_EXACT_NUMERIC =
      family(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.EXACT_NUMERIC);

  public static final SqlSingleOperandTypeChecker BINARY =
      family(SqlTypeFamily.BINARY);

  public static final SqlSingleOperandTypeChecker STRING =
      family(SqlTypeFamily.STRING);

  public static final FamilyOperandTypeChecker STRING_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING);

  public static final FamilyOperandTypeChecker STRING_STRING_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING);

  public static final FamilyOperandTypeChecker STRING_STRING_OPTIONAL_STRING =
      family(ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
          // Third operand optional (operand index 0, 1, 2)
          number -> number == 2);

  public static final SqlSingleOperandTypeChecker CHARACTER =
      family(SqlTypeFamily.CHARACTER);

  public static final SqlSingleOperandTypeChecker DATETIME =
      family(SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker DATE =
      family(SqlTypeFamily.DATE);

  public static final SqlSingleOperandTypeChecker TIME =
      family(SqlTypeFamily.TIME);

  public static final SqlSingleOperandTypeChecker TIMESTAMP =
      family(SqlTypeFamily.TIMESTAMP);

  public static final SqlSingleOperandTypeChecker INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker CHARACTER_CHARACTER_DATETIME =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker PERIOD =
      new PeriodOperandTypeChecker();

  public static final SqlSingleOperandTypeChecker PERIOD_OR_DATETIME =
      PERIOD.or(DATETIME);

  public static final FamilyOperandTypeChecker INTERVAL_INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker MULTISET =
      family(SqlTypeFamily.MULTISET);

  public static final SqlSingleOperandTypeChecker ARRAY =
      family(SqlTypeFamily.ARRAY);
  /** Checks that returns whether a value is a multiset or an array.
   * Cf Java, where list and set are collections but a map is not. */
  public static final SqlSingleOperandTypeChecker COLLECTION =
      family(SqlTypeFamily.MULTISET).or(family(SqlTypeFamily.ARRAY));

  public static final SqlSingleOperandTypeChecker COLLECTION_OR_MAP =
      family(SqlTypeFamily.MULTISET)
          .or(family(SqlTypeFamily.ARRAY))
          .or(family(SqlTypeFamily.MAP));

  /**
   * Operand type-checking strategy where type must be a literal or NULL.
   */
  public static final SqlSingleOperandTypeChecker NULLABLE_LITERAL =
      new LiteralOperandTypeChecker(true);

  /**
   * Operand type-checking strategy type must be a non-NULL literal.
   */
  public static final SqlSingleOperandTypeChecker LITERAL =
      new LiteralOperandTypeChecker(false);

  /**
   * Operand type-checking strategy type must be a positive integer non-NULL
   * literal.
   */
  public static final SqlSingleOperandTypeChecker POSITIVE_INTEGER_LITERAL =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.INTEGER),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          if (!LITERAL.checkSingleOperandType(
              callBinding,
              node,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          if (!super.checkSingleOperandType(
              callBinding,
              node,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          final SqlLiteral arg = (SqlLiteral) node;
          final BigDecimal value = arg.getValueAs(BigDecimal.class);
          if (value.compareTo(BigDecimal.ZERO) < 0
              || hasFractionalPart(value)) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  RESOURCE.argumentMustBePositiveInteger(
                      callBinding.getOperator().getName()));
            }
            return false;
          }
          if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  RESOURCE.numberLiteralOutOfRange(value.toString()));
            }
            return false;
          }
          return true;
        }

        /** Returns whether a number has any fractional part.
         *
         * @see BigDecimal#longValueExact() */
        private boolean hasFractionalPart(BigDecimal bd) {
          return bd.precision() - bd.scale() <= 0;
        }
      };

  /**
   * Operand type-checking strategy type must be a numeric non-NULL
   * literal in the range 0 and 1 inclusive.
   */
  public static final SqlSingleOperandTypeChecker UNIT_INTERVAL_NUMERIC_LITERAL =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.NUMERIC),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          if (!LITERAL.checkSingleOperandType(
              callBinding,
              node,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          if (!super.checkSingleOperandType(
              callBinding,
              node,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          final SqlLiteral arg = (SqlLiteral) node;
          final BigDecimal value = arg.getValueAs(BigDecimal.class);
          if (value.compareTo(BigDecimal.ZERO) < 0
              || value.compareTo(BigDecimal.ONE) > 0) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  RESOURCE.argumentMustBeNumericLiteralInRange(
                      callBinding.getOperator().getName(), 0, 1));
            }
            return false;
          }
          return true;
        }
      };

  /**
   * Operand type-checking strategy where two operands must both be in the
   * same type family.
   */
  public static final SqlSingleOperandTypeChecker SAME_SAME =
      new SameOperandTypeChecker(2);

  public static final SqlSingleOperandTypeChecker SAME_SAME_INTEGER =
      new SameOperandTypeExceptLastOperandChecker(3, "INTEGER");

  /**
   * Operand type-checking strategy where three operands must all be in the
   * same type family.
   */
  public static final SqlSingleOperandTypeChecker SAME_SAME_SAME =
      new SameOperandTypeChecker(3);

  /**
   * Operand type-checking strategy where any number of operands must all be
   * in the same type family.
   */
  public static final SqlOperandTypeChecker SAME_VARIADIC =
      new SameOperandTypeChecker(-1);

  /**
   * Operand type-checking strategy where any positive number of operands must all be
   * in the same type family.
   */
  public static final SqlOperandTypeChecker AT_LEAST_ONE_SAME_VARIADIC =
      new SameOperandTypeChecker(-1) {
        @Override public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.from(1);
        }
      };

  /**
   * Operand type-checking strategy where a given list of operands must all
   * have the same type.
   */
  public static SqlSingleOperandTypeChecker same(int operandCount,
      int... ordinals) {
    final ImmutableIntList ordinalList = ImmutableIntList.of(ordinals);
    checkArgument(ordinalList.size() >= 2);
    checkArgument(Util.isDistinct(ordinalList));
    return new SameOperandTypeChecker(operandCount) {
      @Override protected List<Integer> getOperandList(int operandCount) {
        return ordinalList;
      }
    };
  }

  /**
   * Operand type-checking strategy where operand types must allow ordered
   * comparisons.
   */
  public static final SqlOperandTypeChecker COMPARABLE_ORDERED_COMPARABLE_ORDERED =
      new ComparableOperandTypeChecker(2, RelDataTypeComparability.ALL,
          SqlOperandTypeChecker.Consistency.COMPARE);

  /**
   * Operand type-checking strategy where operand type must allow ordered
   * comparisons. Used when instance comparisons are made on single operand
   * functions
   */
  public static final SqlOperandTypeChecker COMPARABLE_ORDERED =
      new ComparableOperandTypeChecker(1, RelDataTypeComparability.ALL,
          SqlOperandTypeChecker.Consistency.NONE);

  /**
   * Operand type-checking strategy where operand types must allow unordered
   * comparisons.
   */
  public static final SqlOperandTypeChecker COMPARABLE_UNORDERED_COMPARABLE_UNORDERED =
      new ComparableOperandTypeChecker(2, RelDataTypeComparability.UNORDERED,
          SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE);

  /**
   * Operand type-checking strategy where two operands must both be in the
   * same string type family.
   */
  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME =
      STRING_STRING.and(SAME_SAME);

  /**
   * Operand type-checking strategy where three operands must all be in the
   * same string type family.
   */
  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME_SAME =
      STRING_STRING_STRING.and(SAME_SAME_SAME);

  public static final SqlSingleOperandTypeChecker STRING_STRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_STRING_INTEGER_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
          SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
          SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER_OPTIONAL_INTEGER =
      family(
          ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
              SqlTypeFamily.INTEGER), i -> i == 2);

  public static final SqlSingleOperandTypeChecker STRING_NUMERIC =
      family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC);

  public static final SqlSingleOperandTypeChecker STRING_NUMERIC_NUMERIC =
      family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);

  /** Operand type-checking strategy where the first operand is a character or
   * binary string (CHAR, VARCHAR, BINARY or VARBINARY), and the second operand
   * is INTEGER. */
  public static final SqlSingleOperandTypeChecker CBSTRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)
          .or(family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER));

  /**
   * Operand type-checking strategy where two operands must both be in the
   * same string type family and last type is INTEGER.
   */
  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME_INTEGER =
      STRING_STRING_INTEGER.and(SAME_SAME_INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME_OR_ARRAY_SAME_SAME =
      or(STRING_SAME_SAME,
          and(OperandTypes.SAME_SAME, family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));

  public static final SqlSingleOperandTypeChecker ANY =
      family(SqlTypeFamily.ANY);

  public static final SqlSingleOperandTypeChecker ANY_ANY =
      family(SqlTypeFamily.ANY, SqlTypeFamily.ANY);
  public static final SqlSingleOperandTypeChecker ANY_IGNORE =
      family(SqlTypeFamily.ANY, SqlTypeFamily.IGNORE);
  public static final SqlSingleOperandTypeChecker IGNORE_ANY =
      family(SqlTypeFamily.IGNORE, SqlTypeFamily.ANY);
  public static final SqlSingleOperandTypeChecker ANY_NUMERIC =
      family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC);
  public static final SqlSingleOperandTypeChecker ANY_NUMERIC_ANY =
      family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY);
  public static final SqlSingleOperandTypeChecker ANY_STRING_STRING =
      family(SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker CURSOR =
      family(SqlTypeFamily.CURSOR);

  public static final SqlOperandTypeChecker MEASURE =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.ANY),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode node,
            int iFormalOperand, boolean throwOnFailure) {
          if (!super.checkSingleOperandType(callBinding, node, iFormalOperand,
              throwOnFailure)) {
            return false;
          }
          // Scope is non-null at validate time, which is when we need to make
          // this check.
          final @Nullable SqlValidatorScope scope = callBinding.getScope();
          if (scope != null && !scope.isMeasureRef(node)) {
            if (throwOnFailure) {
              throw callBinding.newValidationError(
                  RESOURCE.argumentMustBeMeasure(
                      callBinding.getOperator().getName()));
            }
            return false;
          }
          return true;
        }
      };

  /**
   * Parameter type-checking strategy where type must a nullable time interval,
   * nullable time interval.
   */
  public static final SqlSingleOperandTypeChecker INTERVAL_SAME_SAME =
      INTERVAL_INTERVAL.and(SAME_SAME);

  public static final SqlSingleOperandTypeChecker NUMERIC_INTERVAL =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker INTERVAL_NUMERIC =
      family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.NUMERIC);

  public static final SqlSingleOperandTypeChecker TIME_INTERVAL =
      family(SqlTypeFamily.TIME, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker TIMESTAMP_INTERVAL =
      family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL =
      family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL_INTERVAL =
      family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
          SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL_INTERVAL_TIME =
      family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
          SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.TIME);

  public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL_TIME =
      family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
          SqlTypeFamily.TIME);

  public static final SqlSingleOperandTypeChecker INTERVAL_DATETIME =
      family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker INTERVALINTERVAL_INTERVALDATETIME =
      INTERVAL_SAME_SAME.or(INTERVAL_DATETIME);

  // TODO: datetime+interval checking missing
  // TODO: interval+datetime checking missing
  public static final SqlSingleOperandTypeChecker PLUS_OPERATOR =
      NUMERIC_NUMERIC
          .or(INTERVAL_SAME_SAME)
          .or(DATETIME_INTERVAL)
          .or(INTERVAL_DATETIME);

  /**
   * Type-checking strategy for the "*" operator.
   */
  public static final SqlSingleOperandTypeChecker MULTIPLY_OPERATOR =
      NUMERIC_NUMERIC
          .or(INTERVAL_NUMERIC)
          .or(NUMERIC_INTERVAL);

  /**
   * Type-checking strategy for the "/" operator.
   */
  public static final SqlSingleOperandTypeChecker DIVISION_OPERATOR =
      NUMERIC_NUMERIC.or(INTERVAL_NUMERIC);

  public static final SqlSingleOperandTypeChecker MINUS_OPERATOR =
      // TODO:  compatibility check
      NUMERIC_NUMERIC.or(INTERVAL_SAME_SAME).or(DATETIME_INTERVAL);

  public static final FamilyOperandTypeChecker MINUS_DATE_OPERATOR =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME,
              SqlTypeFamily.DATETIME_INTERVAL),
          i -> false) {
        @Override public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
          }
          return SAME_SAME.checkOperandTypes(callBinding, throwOnFailure);
        }
      };

  public static final SqlSingleOperandTypeChecker NUMERIC_OR_INTERVAL =
      NUMERIC.or(INTERVAL);

  public static final SqlSingleOperandTypeChecker NUMERIC_OR_STRING =
      NUMERIC.or(STRING);

  /** Checker that returns whether a value is a multiset of records or an
   * array of records.
   *
   * @see #COLLECTION */
  public static final SqlSingleOperandTypeChecker RECORD_COLLECTION =
      new RecordTypeWithOneFieldChecker(
          sqlTypeName ->
              sqlTypeName != SqlTypeName.ARRAY && sqlTypeName != SqlTypeName.MULTISET) {

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return "UNNEST(<MULTISET>)";
        }
      };


  /**
   * Checker for record just has one field.
   */
  private abstract static class RecordTypeWithOneFieldChecker
      implements SqlSingleOperandTypeChecker {

    private final Predicate<SqlTypeName> typeNamePredicate;

    private RecordTypeWithOneFieldChecker(Predicate<SqlTypeName> predicate) {
      this.typeNamePredicate = predicate;
    }

    @Override public boolean checkSingleOperandType(
        SqlCallBinding callBinding,
        SqlNode node,
        int iFormalOperand,
        boolean throwOnFailure) {
      assert 0 == iFormalOperand;
      RelDataType type = SqlTypeUtil.deriveType(callBinding, node);
      boolean validationError = false;
      if (!type.isStruct()) {
        validationError = true;
      } else if (type.getFieldList().size() != 1) {
        validationError = true;
      } else {
        SqlTypeName typeName =
            type.getFieldList().get(0).getType().getSqlTypeName();
        if (typeNamePredicate.test(typeName)) {
          validationError = true;
        }
      }

      if (validationError && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return !validationError;
    }
  }

  /** Checker that returns whether a value is a collection (multiset or array)
   * of scalar or record values. */
  public static final SqlSingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION =
      COLLECTION.or(RECORD_COLLECTION);

  public static final SqlSingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION_OR_MAP =
      COLLECTION_OR_MAP.or(
          new RecordTypeWithOneFieldChecker(
              sqlTypeName ->
                  sqlTypeName != SqlTypeName.MULTISET
                      && sqlTypeName != SqlTypeName.ARRAY
                      && sqlTypeName != SqlTypeName.MAP) {

          @Override public String getAllowedSignatures(SqlOperator op, String opName) {
            return "UNNEST(<MULTISET>)\nUNNEST(<ARRAY>)\nUNNEST(<MAP>)";
          }
        });

  public static final SqlOperandTypeChecker MULTISET_MULTISET =
      new MultisetOperandTypeChecker();

  /**
   * Operand type-checking strategy for a set operator (UNION, INTERSECT,
   * EXCEPT).
   */
  public static final SqlOperandTypeChecker SET_OP =
      new SetopOperandTypeChecker();

  public static final SqlOperandTypeChecker RECORD_TO_SCALAR =
      new SqlSingleOperandTypeChecker() {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          assert 0 == iFormalOperand;
          RelDataType type = SqlTypeUtil.deriveType(callBinding, node);
          boolean validationError = false;
          if (!type.isStruct()) {
            validationError = true;
          } else if (type.getFieldList().size() != 1) {
            validationError = true;
          }

          if (validationError && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          }
          return !validationError;
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return SqlUtil.getAliasedSignature(op, opName,
              ImmutableList.of("RECORDTYPE(SINGLE FIELD)"));
        }
      };

  /** Operand type-checker that accepts period types. Examples:
   *
   * <ul>
   * <li>PERIOD (DATETIME, DATETIME)
   * <li>PERIOD (DATETIME, INTERVAL)
   * <li>[ROW] (DATETIME, DATETIME)
   * <li>[ROW] (DATETIME, INTERVAL)
   * </ul> */
  private static class PeriodOperandTypeChecker
      implements SqlSingleOperandTypeChecker {
    @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
        SqlNode node, int iFormalOperand, boolean throwOnFailure) {
      assert 0 == iFormalOperand;
      RelDataType type = SqlTypeUtil.deriveType(callBinding, node);
      boolean valid = false;
      if (type.isStruct() && type.getFieldList().size() == 2) {
        final RelDataType t0 = type.getFieldList().get(0).getType();
        final RelDataType t1 = type.getFieldList().get(1).getType();
        if (SqlTypeUtil.isDatetime(t0)) {
          if (SqlTypeUtil.isDatetime(t1)) {
            // t0 must be comparable with t1; (DATE, TIMESTAMP) is not valid
            if (SqlTypeUtil.sameNamedType(t0, t1)) {
              valid = true;
            }
          } else if (SqlTypeUtil.isInterval(t1)) {
            valid = true;
          }
        }
      }

      if (!valid && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return valid;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return SqlUtil.getAliasedSignature(op, opName,
          ImmutableList.of("PERIOD (DATETIME, INTERVAL)",
              "PERIOD (DATETIME, DATETIME)"));
    }
  }
}
