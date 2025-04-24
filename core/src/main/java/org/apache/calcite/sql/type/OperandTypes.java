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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.sql.type.NonNullableAccessors.getKeyTypeOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

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
   *
   * <P>WARNING: This operand checker does not work correctly when optional parameters are
   * specified: see <a href="https://issues.apache.org/jira/browse/CALCITE-6984">[CALCITE-6984]</a>
   * and <a href="https://issues.apache.org/jira/browse/CALCITE-6976">[CALCITE-6976]</a>.
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
   * Creates a checker that passes if the operand is a function with
   * a given return type and parameter types. This method can be used
   * to check a lambda expression.
   */
  public static SqlSingleOperandTypeChecker function(SqlTypeFamily returnTypeFamily,
      SqlTypeFamily... paramTypeFamilies) {
    return new LambdaFamilyOperandTypeChecker(
        returnTypeFamily, ImmutableList.copyOf(paramTypeFamilies));
  }

  /**
   * Creates a checker that passes if the operand is a function with
   * a given return type and parameter types. This method can be used
   * to check a lambda expression.
   */
  public static SqlSingleOperandTypeChecker function(SqlTypeFamily returnTypeFamily,
      List<SqlTypeFamily> paramTypeFamilies) {
    return new LambdaFamilyOperandTypeChecker(returnTypeFamily, paramTypeFamilies);
  }

  /**
   * Creates a single-operand checker that passes if the operand's type has a
   * particular {@link SqlTypeName}.
   */
  public static SqlSingleOperandTypeChecker typeName(SqlTypeName typeName) {
    return new TypeNameChecker(typeName);
  }

  /**
   * Creates a checker that passes if the operand is an interval appropriate for
   * a given date/time type. For example, the time frame HOUR is appropriate for
   * type TIMESTAMP or DATE but not TIME.
   */
  public static SqlSingleOperandTypeChecker interval(
      Iterable<TimeUnitRange> ranges) {
    final Set<TimeUnitRange> set = ImmutableSet.copyOf(ranges);
    return new IntervalOperandTypeChecker(intervalQualifier ->
        set.contains(intervalQualifier.timeUnitRange));
  }

  /**
   * Creates a checker for DATE intervals (YEAR, WEEK, ISOWEEK,
   * WEEK_WEDNESDAY, etc.)
   */
  public static SqlSingleOperandTypeChecker dateInterval() {
    return new IntervalOperandTypeChecker(SqlIntervalQualifier::isDate);
  }

  /**
   * Creates a checker for TIME intervals (HOUR, SECOND, etc.)
   */
  public static SqlSingleOperandTypeChecker timeInterval() {
    return new IntervalOperandTypeChecker(SqlIntervalQualifier::isTime);
  }

  /**
   * Creates a checker for TIMESTAMP intervals (YEAR, WEEK, ISOWEEK,
   * WEEK_WEDNESDAY, HOUR, SECOND, etc.)
   */
  public static SqlSingleOperandTypeChecker timestampInterval() {
    return new IntervalOperandTypeChecker(SqlIntervalQualifier::isTimestamp);
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
   * Creates an operand checker from a sequence of single-operand checkers,
   * generating the signature from the components.
   */
  public static SqlOperandTypeChecker sequence(
      BiFunction<SqlOperator, String, String> signatureGenerator,
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.SEQUENCE,
        ImmutableList.copyOf(rules), null, signatureGenerator, null);
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

  public static final SqlSingleOperandTypeChecker INTEGER_INTEGER =
      family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker VARIANT =
      family(SqlTypeFamily.VARIANT);

  public static final SqlSingleOperandTypeChecker NUMERIC_NUMERIC =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);

  public static final SqlSingleOperandTypeChecker NUMERIC_OPTIONAL_NUMERIC =
      NUMERIC.or(NUMERIC_NUMERIC);

  public static final SqlSingleOperandTypeChecker NUMERIC_INTEGER =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker NUMERIC_OPTIONAL_INTEGER =
      NUMERIC.or(NUMERIC_INTEGER);

  public static final SqlOperandTypeChecker NUMERIC_INT32 =
      sequence(
          (operator, name) -> operator.getName() + "(<NUMERIC>, <INTEGER>)",
          family(SqlTypeFamily.NUMERIC),
          // Only 32-bit integer allowed for the second argument
          new TypeNameChecker(SqlTypeName.INTEGER));

  public static final SqlSingleOperandTypeChecker NUMERIC_CHARACTER =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER);

  public static final SqlSingleOperandTypeChecker EXACT_NUMERIC =
      family(SqlTypeFamily.EXACT_NUMERIC);

  public static final SqlSingleOperandTypeChecker EXACT_NUMERIC_EXACT_NUMERIC =
      family(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.EXACT_NUMERIC);

  public static final SqlSingleOperandTypeChecker BINARY =
      family(SqlTypeFamily.BINARY);

  public static final SqlSingleOperandTypeChecker BINARY_BINARY =
      family(SqlTypeFamily.BINARY, SqlTypeFamily.BINARY);

  public static final SqlSingleOperandTypeChecker STRING =
      family(SqlTypeFamily.STRING);

  public static final FamilyOperandTypeChecker STRING_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker STRING_OPTIONAL_STRING =
      STRING.or(STRING_STRING);

  public static final FamilyOperandTypeChecker STRING_STRING_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker STRING_STRING_OPTIONAL_STRING =
      STRING_STRING.or(STRING_STRING_STRING);

  public static final SqlSingleOperandTypeChecker STRING_OPTIONAL_STRING_OPTIONAL_STRING =
      // operands 1 and 2 are optional
      STRING
          .or(STRING_STRING)
          .or(STRING_STRING_STRING);

  public static final FamilyOperandTypeChecker STRING_STRING_STRING_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING,
          SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker STRING_NUMERIC =
      family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC);

  static final SqlSingleOperandTypeChecker STRING_NUMERIC_STRING =
      family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING);

  public static final SqlSingleOperandTypeChecker STRING_NUMERIC_OPTIONAL_STRING =
      STRING_NUMERIC.or(STRING_NUMERIC_STRING);

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

  public static final SqlSingleOperandTypeChecker DATE_OR_TIMESTAMP =
      DATE.or(TIMESTAMP);

  /** Type-checker that matches "TIMESTAMP WITH LOCAL TIME ZONE" but not other
   * members of the "TIMESTAMP" family (e.g. "TIMESTAMP"). */
  public static final SqlSingleOperandTypeChecker TIMESTAMP_LTZ =
      new TypeNameChecker(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

  /** Type-checker that matches "TIMESTAMP" but not other members of the
   * "TIMESTAMP" family (e.g. "TIMESTAMP WITH LOCAL TIME ZONE"). */
  public static final SqlSingleOperandTypeChecker TIMESTAMP_NTZ =
      new TypeNameChecker(SqlTypeName.TIMESTAMP);

  public static final SqlSingleOperandTypeChecker INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker CHARACTER_CHARACTER =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER);

  public static final SqlSingleOperandTypeChecker CHARACTER_CHARACTER_DATETIME =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker CHARACTER_DATE =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATE);

  public static final SqlSingleOperandTypeChecker CHARACTER_TIME =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIME);

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

  public static final SqlSingleOperandTypeChecker ARRAY_ARRAY =
      family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY);

  public static final SqlSingleOperandTypeChecker ARRAY_OR_MAP =
      OperandTypes.family(SqlTypeFamily.ARRAY)
          .or(OperandTypes.family(SqlTypeFamily.MAP))
          .or(OperandTypes.family(SqlTypeFamily.ANY));

  public static final SqlSingleOperandTypeChecker ARRAY_OR_MAP_OR_VARIANT =
      OperandTypes.family(SqlTypeFamily.ARRAY)
          .or(OperandTypes.family(SqlTypeFamily.MAP))
          .or(OperandTypes.family(SqlTypeFamily.VARIANT))
          .or(OperandTypes.family(SqlTypeFamily.ANY));

  public static final SqlOperandTypeChecker STRING_ARRAY_CHARACTER_OPTIONAL_CHARACTER =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
          i -> i == 2) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
          }
          RelDataType elementType = callBinding.getOperandType(0).getComponentType();
          if (elementType == null || !SqlTypeUtil.isString(elementType)) {
            if (throwOnFailure) {
              throw callBinding.newValidationSignatureError();
            }
            return false;
          }
          return true;
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(<STRING ARRAY>, <CHARACTER>[, <CHARACTER>])";
        }
      };

  public static final SqlSingleOperandTypeChecker ARRAY_OF_INTEGER =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.ARRAY), i -> false) {
        @Override public boolean checkSingleOperandType(SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, SqlTypeFamily family, boolean throwOnFailure) {
          if (!super.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              family,
              throwOnFailure)) {
            return false;
          }
          RelDataType type = SqlTypeUtil.deriveType(callBinding, operand);
          if (SqlTypeUtil.isNull(type)) {
            return true;
          }
          RelDataType componentType =
              requireNonNull(type.getComponentType(), "componentType");
          if (SqlTypeUtil.isIntType(componentType) || SqlTypeUtil.isNull(componentType)) {
            return true;
          }

          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          }
          return false;
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(<INTEGER ARRAY>)";
        }
      };

  /** Operand type-checking strategy that the first argument is of string type,
   * and the remaining arguments can be any type. */
  public static final SqlOperandTypeChecker STRING_FIRST_OBJECT_REPEAT =
      new StringFirstAndRepeatOperandTypeChecker(
          SqlOperandCountRanges.from(2), "(<STRING>(,<ANY>)+)") {
        @Override public boolean checkRepeatOperandTypes(SqlCallBinding callBinding,
            boolean throwOnFailure) {
          ImmutableList.Builder<SqlTypeFamily> builder = ImmutableList.builder();
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            TypeCoercion coercion = callBinding.getValidator().getTypeCoercion();
            RelDataType operandType = callBinding.getOperandType(i);
            RelDataType cast =
                ((AbstractTypeCoercion) coercion).implicitCast(operandType, SqlTypeFamily.STRING);
            SqlTypeFamily family =
                cast != null ? SqlTypeFamily.STRING
                    : operandType.getSqlTypeName().getFamily();
            requireNonNull(family, "family");
            builder.add(family);
          }
          ImmutableList<SqlTypeFamily> families = builder.build();
          return family(families).checkOperandTypes(callBinding, throwOnFailure);
        }
      };

  /** Operand type-checking strategy that the first argument is of string type,
   * and the remaining arguments can be of string or array of string type. */
  public static final SqlOperandTypeChecker STRING_FIRST_STRING_ARRAY_REPEAT =
      new StringFirstAndRepeatOperandTypeChecker(
          SqlOperandCountRanges.from(1), "(<STRING>[,<STRING> | ARRAY<STRING>]+)") {
        @Override public boolean checkRepeatOperandTypes(SqlCallBinding callBinding,
            boolean throwOnFailure) {
          // Check Array element is String type
          if (!checkArrayString(callBinding, throwOnFailure)) {
            return false;
          }
          // Check Operand Types with Type Coercion
          return checkFamilyOperandTypes(callBinding, throwOnFailure);
        }

        private boolean checkFamilyOperandTypes(SqlCallBinding callBinding,
            boolean throwOnFailure) {
          ImmutableList.Builder<SqlTypeFamily> builder = ImmutableList.builder();
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            boolean isArray = callBinding.getOperandType(i).getSqlTypeName() == SqlTypeName.ARRAY;
            SqlTypeFamily family = isArray ? SqlTypeFamily.ARRAY : SqlTypeFamily.STRING;
            builder.add(family);
          }
          ImmutableList<SqlTypeFamily> families = builder.build();
          return family(families).checkOperandTypes(callBinding, throwOnFailure);
        }

        private boolean checkArrayString(SqlCallBinding binding, boolean throwOnFailure) {
          for (int i = 0; i < binding.getOperandCount(); i++) {
            RelDataType type = binding.getOperandType(i);
            RelDataType componentType = type.getComponentType();
            boolean isString = componentType != null
                && (SqlTypeUtil.isNull(componentType) || SqlTypeUtil.isString(componentType));
            if (type.getSqlTypeName() == SqlTypeName.ARRAY && !isString) {
              if (throwOnFailure) {
                throw binding.newValidationSignatureError();
              }
              return false;
            }
          }
          return true;
        }
      };

  /** Operand type-checking strategy where the first argument is of string type,
   * and the remaining arguments follow a repeating type pattern.
   *
   * <p>The method {@link #checkRepeatOperandTypes} is an abstract method designed
   * to check the types of these repeating arguments. */
  abstract static class StringFirstAndRepeatOperandTypeChecker implements SqlOperandTypeChecker {

    private final SqlOperandCountRange countRange;
    private final String signatures;

    StringFirstAndRepeatOperandTypeChecker(
        SqlOperandCountRange countRange,
        String signatures) {
      this.countRange = countRange;
      this.signatures = signatures;
    }

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      // Check first operand is String type
      if (!STRING.checkSingleOperandType(callBinding, callBinding.operand(0), 0,
          throwOnFailure)) {
        return false;
      }
      return checkRepeatOperandTypes(callBinding, throwOnFailure);
    }

    @Override public SqlOperandCountRange getOperandCountRange() {
      return countRange;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + signatures;
    }

    public abstract boolean checkRepeatOperandTypes(SqlCallBinding callBinding,
        boolean throwOnFailure);
  }

  /** Checks that returns whether a value is a multiset or an array.
   * Cf Java, where list and set are collections but a map is not. */
  public static final SqlSingleOperandTypeChecker COLLECTION =
      family(SqlTypeFamily.MULTISET).or(family(SqlTypeFamily.ARRAY));

  public static final SqlSingleOperandTypeChecker COLLECTION_OR_MAP =
      family(SqlTypeFamily.MULTISET)
          .or(family(SqlTypeFamily.ARRAY))
          .or(family(SqlTypeFamily.MAP));

  public static final SqlSingleOperandTypeChecker MAP =
      family(SqlTypeFamily.MAP);

  public static final SqlOperandTypeChecker ARRAY_FUNCTION =
      new ArrayFunctionOperandTypeChecker();

  public static final SqlOperandTypeChecker ARRAY_ELEMENT =
      new ArrayElementOperandTypeChecker(true, true);

  public static final SqlOperandTypeChecker ARRAY_ELEMENT_NONNULL =
      new ArrayElementOperandTypeChecker(false, true);

  /** Type checker that accepts an ARRAY as the first argument, but not
   * an expression with type NULL (i.e. a NULL literal). */
  public static final SqlOperandTypeChecker ARRAY_NONNULL =
      family(SqlTypeFamily.ARRAY).and(new NotNullOperandTypeChecker(1, false));

  public static final SqlOperandTypeChecker ARRAY_INSERT =
      new ArrayInsertOperandTypeChecker();

  public static final SqlSingleOperandTypeChecker MAP_FROM_ENTRIES =
      new MapFromEntriesOperandTypeChecker();

  public static final SqlSingleOperandTypeChecker MAP_FUNCTION =
      new MapFunctionOperandTypeChecker();

  public static final SqlOperandTypeChecker MAP_KEY =
      new MapKeyOperandTypeChecker();

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
   * Operand type-checking strategy where all types must be non-NULL value.
   */
  public static final SqlSingleOperandTypeChecker NONNULL_NONNULL =
      new NotNullOperandTypeChecker(2, false);

  /**
   * Operand type-checking strategy type must be a boolean non-NULL literal.
   */
  public static final SqlSingleOperandTypeChecker BOOLEAN_LITERAL =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.BOOLEAN),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode operand,
            int iFormalOperand,
            SqlTypeFamily family,
            boolean throwOnFailure) {
          if (!LITERAL.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          if (!super.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              family,
              throwOnFailure)) {
            return false;
          }

          final SqlLiteral arg = (SqlLiteral) operand;
          final boolean isBooleanLiteral =
              SqlLiteral.valueMatchesType(arg.getValue(), SqlTypeName.BOOLEAN);

          if (!isBooleanLiteral) {
            if (throwOnFailure) {
              throw callBinding.newError(
                  RESOURCE.argumentMustBeBooleanLiteral(
                      callBinding.getOperator().getName()));
            }
            return false;
          }
          return true;
        }
      };

  /**
   * Operand type-checking strategy where the first operand must be array and
   * the second operand must be a boolean non-NULL literal.
   */
  public static final SqlSingleOperandTypeChecker
      ARRAY_BOOLEAN_LITERAL =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.BOOLEAN),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, SqlTypeFamily family,
            boolean throwOnFailure) {
          if (iFormalOperand == 0) {
            return super.checkSingleOperandType(callBinding, operand,
                iFormalOperand, family, throwOnFailure);
          }

          return BOOLEAN_LITERAL.checkSingleOperandType(
              callBinding, operand, iFormalOperand, throwOnFailure);
        }
      };

  /**
   * Operand type-checking strategy type must be a positive integer non-NULL
   * literal.
   */
  public static final SqlSingleOperandTypeChecker POSITIVE_INTEGER_LITERAL =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.INTEGER),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode operand,
            int iFormalOperand,
            SqlTypeFamily family,
            boolean throwOnFailure) {
          if (!LITERAL.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              throwOnFailure)) {
            return false;
          }

          if (!super.checkSingleOperandType(
              callBinding,
              operand,
              iFormalOperand,
              family,
              throwOnFailure)) {
            return false;
          }

          final SqlLiteral arg = (SqlLiteral) operand;
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
   * Operand type-checking strategy where type must be a numeric non-NULL
   * literal in the range 0 to 1 inclusive.
   */
  public static final SqlSingleOperandTypeChecker UNIT_INTERVAL_NUMERIC_LITERAL =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.NUMERIC),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, SqlTypeFamily family, boolean throwOnFailure) {
          if (!LITERAL.checkSingleOperandType(callBinding, operand,
              0, throwOnFailure)) {
            return false;
          }

          if (!super.checkSingleOperandType(callBinding, operand,
              iFormalOperand, family, throwOnFailure)) {
            return false;
          }

          final SqlLiteral arg = (SqlLiteral) operand;
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
   * Operand type-checking strategy where the first operand must be numeric and
   * the second operand must be a numeric non-NULL literal in the range 0 to 1
   * inclusive.
   */
  public static final SqlSingleOperandTypeChecker
      NUMERIC_UNIT_INTERVAL_NUMERIC_LITERAL =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, SqlTypeFamily family, boolean throwOnFailure) {
          if (iFormalOperand == 0) {
            return super.checkSingleOperandType(callBinding, operand,
                iFormalOperand, family, throwOnFailure);
          }

          return UNIT_INTERVAL_NUMERIC_LITERAL.checkSingleOperandType(
              callBinding, operand, iFormalOperand, throwOnFailure);
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

  public static final SqlSingleOperandTypeChecker STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER =
      STRING_STRING
          .or(STRING_STRING_INTEGER)
          .or(STRING_STRING_INTEGER_INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_STRING_INTEGER_INTEGER_INTEGER =
      family(
          ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
              SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER));

  public static final SqlSingleOperandTypeChecker
      STRING_STRING_OPTIONAL_INTEGER_OPTIONAL_INTEGER_OPTIONAL_INTEGER =
      STRING_STRING
          .or(STRING_STRING_INTEGER)
          .or(STRING_STRING_INTEGER_INTEGER)
          .or(STRING_STRING_INTEGER_INTEGER_INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
          SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER_OPTIONAL_INTEGER =
      STRING_INTEGER.or(STRING_INTEGER_INTEGER);

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

  /**
   * Operand type-checking strategy where the second and third operands must be comparable.
   * This is used when the operator has three operands and only the
   * second and third operands need to be comparable.
   */
  public static final SqlSingleOperandTypeChecker SECOND_THIRD_SAME =
      new SameOperandTypeChecker(3) {
        @Override protected List<Integer> getOperandList(int operandCount) {
          // Only check the second and third operands
          return ImmutableList.of(1, 2);
        }
      };
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
  public static final SqlSingleOperandTypeChecker ANY_STRING_OPTIONAL_STRING =
      family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.STRING))
          .or(ANY_STRING_STRING);

  /**
   * Operand type-checking strategy used by {@code ARG_MIN(value, comp)} and
   * similar functions, where the first operand can have any type and the second
   * must be comparable.
   */
  public static final SqlOperandTypeChecker ANY_COMPARABLE =
      new SqlOperandTypeChecker() {
        @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
            boolean throwOnFailure) {
          getOperandCountRange().isValidCount(callBinding.getOperandCount());
          RelDataType type = callBinding.getOperandType(1);
          return type.getComparability() == RelDataTypeComparability.ALL;
        }

        @Override public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(2);
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(<ANY>, <COMPARABLE_TYPE>)";
        }
  };

  public static final SqlSingleOperandTypeChecker CURSOR =
      family(SqlTypeFamily.CURSOR);

  public static final SqlSingleOperandTypeChecker MEASURE =
      new FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.ANY),
          i -> false) {
        @Override public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode operand,
            int iFormalOperand, SqlTypeFamily family, boolean throwOnFailure) {
          if (!super.checkSingleOperandType(callBinding, operand, iFormalOperand, family,
              throwOnFailure)) {
            return false;
          }
          final SqlValidatorScope scope = callBinding.getScope();
          if (!scope.isMeasureRef(operand)) {
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

  public static final SqlOperandTypeChecker MEASURE_BOOLEAN =
      sequence("'<MEASURE>, <BOOLEAN>'", MEASURE, BOOLEAN);

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

  public static final SqlSingleOperandTypeChecker DATE_INTERVAL =
      family(SqlTypeFamily.DATE, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker DATE_ANY =
      family(SqlTypeFamily.DATE, SqlTypeFamily.ANY);

  public static final SqlSingleOperandTypeChecker DATE_CHARACTER =
      family(SqlTypeFamily.DATE, SqlTypeFamily.CHARACTER);

  public static final SqlSingleOperandTypeChecker DATE_TIME =
      family(SqlTypeFamily.DATE, SqlTypeFamily.TIME);

  public static final SqlSingleOperandTypeChecker DATETIME_INTERVAL =
      family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker TIMESTAMP_STRING =
      family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING);

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
      INTERVAL_SAME_SAME.or(INTERVAL_DATETIME)
          .or(family(SqlTypeFamily.INTERVAL_DAY_TIME, SqlTypeFamily.INTERVAL_YEAR_MONTH));

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

        @Override public boolean checkOperandTypesWithoutTypeCoercion(
            final SqlCallBinding callBinding,
            final boolean throwOnFailure) {
          if (!super.checkOperandTypesWithoutTypeCoercion(callBinding, throwOnFailure)) {
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

  public static final SqlOperandTypeChecker EXISTS =
      new SqlOperandTypeChecker() {
        @Override public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          // The first operand must be an array type
          ARRAY.checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure);
          final RelDataType arrayType =
              SqlTypeUtil.deriveType(callBinding, callBinding.operand(0));
          final RelDataType componentType =
              requireNonNull(arrayType.getComponentType(), "componentType");

          // The second operand is a function(array_element_type)->boolean type
          LambdaRelOperandTypeChecker lambdaChecker =
              new LambdaRelOperandTypeChecker(
                  SqlTypeFamily.BOOLEAN,
                  ImmutableList.of(componentType));
          return lambdaChecker.checkSingleOperandType(
              callBinding,
              callBinding.operand(1),
              1,
              throwOnFailure);
        }

        @Override public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(2);
        }

        @Override public String getAllowedSignatures(SqlOperator op, String opName) {
          return "EXISTS(<ARRAY>, <FUNCTION(ARRAY_ELEMENT_TYPE)->BOOLEAN>)";
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

  /** Checker that returns whether a value is a array of record type with two fields. */
  private static class MapFromEntriesOperandTypeChecker
      implements SqlSingleOperandTypeChecker {
    @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
        SqlNode node, int iFormalOperand, boolean throwOnFailure) {
      assert 0 == iFormalOperand;
      RelDataType type = SqlTypeUtil.deriveType(callBinding, node);
      RelDataType componentType = requireNonNull(type.getComponentType(), "componentType");
      boolean valid = false;
      if (type.getSqlTypeName() == SqlTypeName.ARRAY
          && componentType.getSqlTypeName() == SqlTypeName.ROW
          && componentType.getFieldCount() == 2) {
        valid = true;
      }
      if (!valid && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return valid;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return SqlUtil.getAliasedSignature(op, opName,
          ImmutableList.of("ARRAY<RECORDTYPE(TWO FIELDS)>"));
    }
  }

  /**
   * Operand type-checking strategy for a ARRAY function, it allows empty array.
   */
  private static class ArrayFunctionOperandTypeChecker
      extends SameOperandTypeChecker {

    ArrayFunctionOperandTypeChecker() {
      // The args of array are non-fixed, so we set to -1 here. then operandCount
      // can dynamically set according to the number of input args.
      // details please see SameOperandTypeChecker#getOperandList.
      super(-1);
    }

    @Override protected boolean checkOperandTypesImpl(
        SqlOperatorBinding operatorBinding,
        boolean throwOnFailure,
        @Nullable SqlCallBinding callBinding) {
      if (throwOnFailure && callBinding == null) {
        throw new IllegalArgumentException(
            "callBinding must be non-null in case throwOnFailure=true");
      }
      int nOperandsActual = nOperands;
      if (nOperandsActual == -1) {
        nOperandsActual = operatorBinding.getOperandCount();
      }
      RelDataType[] types = new RelDataType[nOperandsActual];
      final List<Integer> operandList =
          getOperandList(operatorBinding.getOperandCount());
      for (int i : operandList) {
        types[i] = operatorBinding.getOperandType(i);
      }
      for (int i : operandList) {
        if (i > 0) {
          // we replace SqlTypeUtil.isComparable with SqlTypeUtil.leastRestrictiveForComparison
          // to handle struct type and NULL constant.
          // details please see: https://issues.apache.org/jira/browse/CALCITE-6163
          RelDataType type =
              SqlTypeUtil.leastRestrictiveForComparison(operatorBinding.getTypeFactory(),
                  types[i], types[i - 1]);
          if (type == null) {
            if (!throwOnFailure) {
              return false;
            }
            throw requireNonNull(callBinding, "callBinding").newValidationError(
                RESOURCE.needSameTypeParameter());
          }
        }
      }
      return true;
    }
  }

  /**
   * Operand type-checking strategy for a MAP function, it allows empty map.
   */
  private static class MapFunctionOperandTypeChecker
      extends SameOperandTypeChecker {

    MapFunctionOperandTypeChecker() {
      // The args of map are non-fixed, so we set to -1 here. then operandCount
      // can dynamically set according to the number of input args.
      // details please see SameOperandTypeChecker#getOperandList.
      super(-1);
    }

    @Override public boolean checkOperandTypes(final SqlCallBinding callBinding,
        final boolean throwOnFailure) {
      final List<RelDataType> argTypes =
          SqlTypeUtil.deriveType(callBinding, callBinding.operands());
      // allows empty map
      if (argTypes.isEmpty()) {
        return true;
      }
      // the size of map arg types must be even.
      if (argTypes.size() % 2 != 0) {
        throw callBinding.newValidationError(RESOURCE.mapRequiresEvenArgCount());
      }
      final Pair<@Nullable RelDataType, @Nullable RelDataType> componentType =
          getComponentTypes(
              callBinding.getTypeFactory(), argTypes);
      // check key type & value type
      if (null == componentType.left || null == componentType.right) {
        if (throwOnFailure) {
          throw callBinding.newValidationError(RESOURCE.needSameTypeParameter());
        }
        return false;
      }
      return true;
    }

    /**
     * Extract the key type and value type of arg types.
     */
    private static Pair<@Nullable RelDataType, @Nullable RelDataType> getComponentTypes(
        RelDataTypeFactory typeFactory,
        List<RelDataType> argTypes) {
      // Util.quotientList(argTypes, 2, 0):
      // This extracts all elements at even indices from argTypes.
      // It represents the types of keys in the map as they are placed at even positions
      // e.g. 0, 2, 4, etc.
      // Symmetrically, Util.quotientList(argTypes, 2, 1) represents odd-indexed elements.
      // details please see Util.quotientList.
      return Pair.of(
          typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
          typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1)));
    }
  }

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

  /**
   * Parameter type-checking strategy where types must be Map and Map key type.
   */
  private static class MapKeyOperandTypeChecker extends SameOperandTypeChecker {
    MapKeyOperandTypeChecker() {
      super(2);
    }

    @Override public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
      final SqlNode op0 = callBinding.operand(0);
      if (!OperandTypes.MAP.checkSingleOperandType(
          callBinding,
          op0,
          0,
          throwOnFailure)) {
        return false;
      }

      final RelDataType mapKeyType =
          getKeyTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op0));
      final SqlNode op1 = callBinding.operand(1);
      RelDataType opType1 = SqlTypeUtil.deriveType(callBinding, op1);

      RelDataType biggest =
          callBinding.getTypeFactory().leastRestrictive(
              ImmutableList.of(mapKeyType, opType1));
      if (biggest == null) {
        if (throwOnFailure) {
          throw callBinding.newError(
              RESOURCE.typeNotComparable(
                  mapKeyType.toString(), opType1.toString()));
        }

        return false;
      }
      return true;
    }
  }

  /** Checker that passes if the operand's type has a particular
   * {@link SqlTypeName}. */
  private static class TypeNameChecker implements SqlSingleOperandTypeChecker,
      ImplicitCastOperandTypeChecker {
    final SqlTypeName typeName;

    TypeNameChecker(SqlTypeName typeName) {
      this.typeName = requireNonNull(typeName, "typeName");
    }

    @Override public boolean checkSingleOperandType(SqlCallBinding callBinding,
        SqlNode operand, int iFormalOperand, boolean throwOnFailure) {
      final RelDataType operandType =
          callBinding.getValidator().getValidatedNodeType(operand);
      return operandType.getSqlTypeName() == typeName;
    }

    @Override public boolean checkOperandTypesWithoutTypeCoercion(
        SqlCallBinding callBinding, boolean throwOnFailure) {
      // FIXME we assume that there is exactly one operand
      return checkSingleOperandType(callBinding, callBinding.operand(0), 0,
          throwOnFailure);
    }

    @Override public SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand) {
      return requireNonNull(typeName.getFamily(), "family");
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(" + typeName.getSpaceName() + ")";
    }
  }

  /**
   * Operand type-checking strategy where the type of the operand is a lambda
   * expression with a given return type and argument {@link SqlTypeFamily}s.
   */
  private static class LambdaFamilyOperandTypeChecker
      extends LambdaOperandTypeChecker {

    private final List<SqlTypeFamily> argFamilies;

    LambdaFamilyOperandTypeChecker(
        SqlTypeFamily returnTypeFamily,
        List<SqlTypeFamily> argFamilies) {
      super(returnTypeFamily);
      this.argFamilies = ImmutableList.copyOf(argFamilies);
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      ImmutableList.Builder<SqlTypeFamily> builder = ImmutableList.builder();
      builder.addAll(argFamilies);
      builder.add(returnTypeFamily);

      return SqlUtil.getAliasedSignature(op, opName, builder.build());
    }

    @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
        boolean throwOnFailure) {
      return false;
    }

    @Override public boolean checkSingleOperandType(
        SqlCallBinding callBinding, SqlNode operand, int iFormalOperand, boolean throwOnFailure) {
      if (!(operand instanceof SqlLambda)
          || ((SqlLambda) operand).getParameters().size() != argFamilies.size()) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }

      final SqlLambda lambdaExpr = (SqlLambda) operand;
      if (SqlUtil.isNullLiteral(lambdaExpr.getExpression(), false)) {
        checkNull(callBinding, lambdaExpr, throwOnFailure);
      }

      final SqlValidator validator = callBinding.getValidator();
      if (!lambdaExpr.getParameters().isEmpty()
          && !argFamilies.stream().allMatch(f -> f == SqlTypeFamily.ANY)) {
        // Replace the parameter types in the lambda expression.
        final SqlLambdaScope scope =
            (SqlLambdaScope) validator.getLambdaScope(lambdaExpr);
        for (int i = 0; i < argFamilies.size(); i++) {
          final SqlNode param = lambdaExpr.getParameters().get(i);
          final RelDataType type =
              argFamilies.get(i).getDefaultConcreteType(callBinding.getTypeFactory());
          if (type != null) {
            scope.getParameterTypes().put(param.toString(), type);
          }
        }
        lambdaExpr.accept(new TypeRemover(validator));
        // Given the new relDataType, re-validate the lambda expression.
        validator.validateLambda(lambdaExpr);
      }

      return checkReturnType(validator, callBinding, lambdaExpr, throwOnFailure);
    }
  }

  /**
   * Operand type-checking strategy where the type of the operand is a lambda
   * expression with a given return type and argument {@link RelDataType}s.
   */
  private static class LambdaRelOperandTypeChecker
      extends LambdaOperandTypeChecker {
    private final List<RelDataType> argTypes;

    LambdaRelOperandTypeChecker(
        SqlTypeFamily returnTypeFamily,
        List<RelDataType> argTypes) {
      super(returnTypeFamily);
      this.argTypes = argTypes;
    }

    @Override public String getAllowedSignatures(SqlOperator op, String opName) {
      ImmutableList.Builder<SqlTypeFamily> builder = ImmutableList.builder();
      argTypes.stream()
          .map(t -> requireNonNull(t.getSqlTypeName().getFamily()))
          .forEach(builder::add);
      builder.add(returnTypeFamily);

      return SqlUtil.getAliasedSignature(op, opName, builder.build());
    }

    @Override public boolean checkSingleOperandType(SqlCallBinding callBinding, SqlNode operand,
        int iFormalOperand,
        boolean throwOnFailure) {
      if (!(operand instanceof SqlLambda)
          || ((SqlLambda) operand).getParameters().size() != argTypes.size()) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }

      final SqlLambda lambdaExpr = (SqlLambda) operand;
      if (SqlUtil.isNullLiteral(lambdaExpr.getExpression(), false)) {
        checkNull(callBinding, lambdaExpr, throwOnFailure);
      }

      // Replace the parameter types in the lambda expression.
      final SqlValidator validator = callBinding.getValidator();
      final SqlLambdaScope scope =
          (SqlLambdaScope) validator.getLambdaScope(lambdaExpr);
      for (int i = 0; i < argTypes.size(); i++) {
        final SqlNode param = lambdaExpr.getParameters().get(i);
        final RelDataType type = argTypes.get(i);
        if (type != null) {
          scope.getParameterTypes().put(param.toString(), type);
        }
      }
      lambdaExpr.accept(new TypeRemover(validator));
      // Given the new relDataType, re-validate the lambda expression.
      validator.validateLambda(lambdaExpr);

      return checkReturnType(validator, callBinding, lambdaExpr, throwOnFailure);
    }
  }

  /**
   * Abstract base class for type-checking strategies involving lambda expressions.
   * This class provides common functionality for checking the type of lambda expression.
   */
  private abstract static class LambdaOperandTypeChecker
      implements SqlSingleOperandTypeChecker {
    protected final SqlTypeFamily returnTypeFamily;

    LambdaOperandTypeChecker(SqlTypeFamily returnTypeFamily) {
      this.returnTypeFamily = requireNonNull(returnTypeFamily, "returnTypeFamily");
    }

    protected boolean checkNull(
        SqlCallBinding callBinding,
        SqlLambda lambdaExpr,
        boolean throwOnFailure) {
      if (callBinding.isTypeCoercionEnabled()) {
        return true;
      }

      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(lambdaExpr.getExpression(),
            RESOURCE.nullIllegal());
      }
      return false;
    }

    protected boolean checkReturnType(
        SqlValidator validator,
        SqlCallBinding callBinding,
        SqlLambda lambdaExpr,
        boolean throwOnFailure) {
      final RelDataType newType = validator.getValidatedNodeType(lambdaExpr);
      assert newType instanceof FunctionSqlType;
      final SqlTypeName returnTypeName =
          ((FunctionSqlType) newType).getReturnType().getSqlTypeName();
      if (returnTypeName == SqlTypeName.ANY
          || returnTypeFamily.getTypeNames().contains(returnTypeName)) {
        return true;
      }

      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }

    /**
     * Visitor that removes the relDataType of a sqlNode and its children in the
     * validator. Now this visitor is only used for removing the relDataType
     * when we check lambda operand. Since lambda expressions will be
     * validated for the second time based on the given parameter type,
     * the type cached during the first validation must be cleared.
     */
    protected static class TypeRemover extends SqlBasicVisitor<Void> {
      private final SqlValidator validator;

      protected TypeRemover(SqlValidator validator) {
        this.validator = validator;
      }

      @Override public Void visit(SqlIdentifier id) {
        validator.removeValidatedNodeType(id);
        return super.visit(id);
      }

      @Override public Void visit(SqlCall call) {
        validator.removeValidatedNodeType(call);
        return super.visit(call);
      }
    }
  }
}
