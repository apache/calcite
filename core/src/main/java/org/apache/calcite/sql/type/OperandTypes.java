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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Strategies for checking operand types.
 *
 * <p>This class defines singleton instances of strategy objects for operand
 * type checking. {@link org.apache.calcite.sql.type.ReturnTypes}
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
   * Creates a checker that passes if any one of the rules passes.
   */
  public static SqlOperandTypeChecker or(SqlOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.OR,
        ImmutableList.copyOf(rules), null, null);
  }

  /**
   * Creates a checker that passes if all of the rules pass.
   */
  public static SqlOperandTypeChecker and(SqlOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.AND,
        ImmutableList.copyOf(rules), null, null);
  }

  /**
   * Creates a single-operand checker that passes if any one of the rules
   * passes.
   */
  public static SqlSingleOperandTypeChecker or(
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeSingleOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.OR,
        ImmutableList.copyOf(rules), null);
  }

  /**
   * Creates a single-operand checker that passes if all of the rules
   * pass.
   */
  public static SqlSingleOperandTypeChecker and(
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeSingleOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.AND,
        ImmutableList.copyOf(rules), null);
  }

  /**
   * Creates an operand checker from a sequence of single-operand checkers.
   */
  public static SqlOperandTypeChecker sequence(String allowedSignatures,
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.SEQUENCE,
        ImmutableList.copyOf(rules), allowedSignatures, null);
  }

  /**
   * Creates a checker that passes if all of the rules pass for each operand,
   * using a given operand count strategy.
   */
  public static SqlOperandTypeChecker repeat(SqlOperandCountRange range,
      SqlSingleOperandTypeChecker... rules) {
    return new CompositeOperandTypeChecker(
        CompositeOperandTypeChecker.Composition.REPEAT,
        ImmutableList.copyOf(rules), null, range);
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
      public boolean checkOperandTypes(
          SqlCallBinding callBinding,
          boolean throwOnFailure) {
        return range.isValidCount(callBinding.getOperandCount());
      }

      public SqlOperandCountRange getOperandCountRange() {
        return range;
      }

      public String getAllowedSignatures(SqlOperator op, String opName) {
        return opName + "(...)";
      }

      public boolean isOptional(int i) {
        return false;
      }

      public Consistency getConsistency() {
        return Consistency.NONE;
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

  public static final SqlSingleOperandTypeChecker CHARACTER =
      family(SqlTypeFamily.CHARACTER);

  public static final SqlSingleOperandTypeChecker DATETIME =
      family(SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker CHARACTER_CHARACTER_DATETIME =
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME);

  public static final SqlSingleOperandTypeChecker PERIOD =
      new PeriodOperandTypeChecker();

  public static final SqlSingleOperandTypeChecker PERIOD_OR_DATETIME =
      or(PERIOD, DATETIME);

  public static final FamilyOperandTypeChecker INTERVAL_INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker MULTISET =
      family(SqlTypeFamily.MULTISET);

  public static final SqlSingleOperandTypeChecker ARRAY =
      family(SqlTypeFamily.ARRAY);

  /** Checks that returns whether a value is a multiset or an array.
   * Cf Java, where list and set are collections but a map is not. */
  public static final SqlSingleOperandTypeChecker COLLECTION =
      or(family(SqlTypeFamily.MULTISET),
          family(SqlTypeFamily.ARRAY));

  public static final SqlSingleOperandTypeChecker COLLECTION_OR_MAP =
      or(family(SqlTypeFamily.MULTISET),
          family(SqlTypeFamily.ARRAY),
          family(SqlTypeFamily.MAP));

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
        public boolean checkSingleOperandType(
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
          final BigDecimal value = (BigDecimal) arg.getValue();
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
      OperandTypes.and(STRING_STRING, SAME_SAME);

  /**
   * Operand type-checking strategy where three operands must all be in the
   * same string type family.
   */
  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME_SAME =
      OperandTypes.and(STRING_STRING_STRING, SAME_SAME_SAME);

  public static final SqlSingleOperandTypeChecker STRING_STRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_STRING_INTEGER_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.STRING,
          SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);

  public static final SqlSingleOperandTypeChecker STRING_INTEGER =
      family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);

  /** Operand type-checking strategy where the first operand is a character or
   * binary string (CHAR, VARCHAR, BINARY or VARBINARY), and the second operand
   * is INTEGER. */
  public static final SqlSingleOperandTypeChecker CBSTRING_INTEGER =
      or(family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
          family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER));

  /**
   * Operand type-checking strategy where two operands must both be in the
   * same string type family and last type is INTEGER.
   */
  public static final SqlSingleOperandTypeChecker STRING_SAME_SAME_INTEGER =
      OperandTypes.and(STRING_STRING_INTEGER, SAME_SAME_INTEGER);

  public static final SqlSingleOperandTypeChecker ANY =
      family(SqlTypeFamily.ANY);

  public static final SqlSingleOperandTypeChecker ANY_ANY =
      family(SqlTypeFamily.ANY, SqlTypeFamily.ANY);
  public static final SqlSingleOperandTypeChecker ANY_NUMERIC =
      family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC);

  /**
   * Parameter type-checking strategy type must a nullable time interval,
   * nullable time interval
   */
  public static final SqlSingleOperandTypeChecker INTERVAL_SAME_SAME =
      OperandTypes.and(INTERVAL_INTERVAL, SAME_SAME);

  public static final SqlSingleOperandTypeChecker NUMERIC_INTERVAL =
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.DATETIME_INTERVAL);

  public static final SqlSingleOperandTypeChecker INTERVAL_NUMERIC =
      family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.NUMERIC);

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
      OperandTypes.or(INTERVAL_SAME_SAME, INTERVAL_DATETIME);

  // TODO: datetime+interval checking missing
  // TODO: interval+datetime checking missing
  public static final SqlSingleOperandTypeChecker PLUS_OPERATOR =
      OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL,
          INTERVAL_DATETIME);

  /**
   * Type checking strategy for the "*" operator
   */
  public static final SqlSingleOperandTypeChecker MULTIPLY_OPERATOR =
      OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_NUMERIC, NUMERIC_INTERVAL);

  /**
   * Type checking strategy for the "/" operator
   */
  public static final SqlSingleOperandTypeChecker DIVISION_OPERATOR =
      OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_NUMERIC);

  public static final SqlSingleOperandTypeChecker MINUS_OPERATOR =
      // TODO:  compatibility check
      OperandTypes.or(NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL);

  public static final FamilyOperandTypeChecker MINUS_DATE_OPERATOR =
      new FamilyOperandTypeChecker(
          ImmutableList.of(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME,
              SqlTypeFamily.DATETIME_INTERVAL),
          i -> false) {
        public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
          }
          return SAME_SAME.checkOperandTypes(callBinding, throwOnFailure);
        }
      };

  public static final SqlSingleOperandTypeChecker NUMERIC_OR_INTERVAL =
      OperandTypes.or(NUMERIC, INTERVAL);

  public static final SqlSingleOperandTypeChecker NUMERIC_OR_STRING =
      OperandTypes.or(NUMERIC, STRING);

  /** Checker that returns whether a value is a multiset of records or an
   * array of records.
   *
   * @see #COLLECTION */
  public static final SqlSingleOperandTypeChecker RECORD_COLLECTION =
      new SqlSingleOperandTypeChecker() {
        public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          assert 0 == iFormalOperand;
          RelDataType type =
              callBinding.getValidator().deriveType(
                  callBinding.getScope(),
                  node);
          boolean validationError = false;
          if (!type.isStruct()) {
            validationError = true;
          } else if (type.getFieldList().size() != 1) {
            validationError = true;
          } else {
            SqlTypeName typeName =
                type.getFieldList().get(0).getType().getSqlTypeName();
            if (typeName != SqlTypeName.MULTISET
                && typeName != SqlTypeName.ARRAY) {
              validationError = true;
            }
          }

          if (validationError && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          }
          return !validationError;
        }

        public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          return checkSingleOperandType(
              callBinding,
              callBinding.operand(0),
              0,
              throwOnFailure);
        }

        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(1);
        }

        public String getAllowedSignatures(SqlOperator op, String opName) {
          return "UNNEST(<MULTISET>)";
        }

        public boolean isOptional(int i) {
          return false;
        }

        public Consistency getConsistency() {
          return Consistency.NONE;
        }
      };

  /** Checker that returns whether a value is a collection (multiset or array)
   * of scalar or record values. */
  public static final SqlSingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION =
      OperandTypes.or(COLLECTION, RECORD_COLLECTION);

  public static final SqlSingleOperandTypeChecker SCALAR_OR_RECORD_COLLECTION_OR_MAP =
      OperandTypes.or(COLLECTION_OR_MAP, RECORD_COLLECTION);

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
        public boolean checkSingleOperandType(
            SqlCallBinding callBinding,
            SqlNode node,
            int iFormalOperand,
            boolean throwOnFailure) {
          assert 0 == iFormalOperand;
          RelDataType type =
              callBinding.getValidator().deriveType(
                  callBinding.getScope(),
                  node);
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

        public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
          return checkSingleOperandType(
              callBinding,
              callBinding.operand(0),
              0,
              throwOnFailure);
        }

        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(1);
        }

        public String getAllowedSignatures(SqlOperator op, String opName) {
          return SqlUtil.getAliasedSignature(op, opName,
              ImmutableList.of("RECORDTYPE(SINGLE FIELD)"));
        }

        public boolean isOptional(int i) {
          return false;
        }

        public Consistency getConsistency() {
          return Consistency.NONE;
        }
      };

  /** Operand type checker that accepts period types:
   * PERIOD (DATETIME, DATETIME)
   * PERIOD (DATETIME, INTERVAL)
   * [ROW] (DATETIME, DATETIME)
   * [ROW] (DATETIME, INTERVAL) */
  private static class PeriodOperandTypeChecker
      implements SqlSingleOperandTypeChecker {
    public boolean checkSingleOperandType(SqlCallBinding callBinding,
        SqlNode node, int iFormalOperand, boolean throwOnFailure) {
      assert 0 == iFormalOperand;
      RelDataType type =
          callBinding.getValidator().deriveType(callBinding.getScope(), node);
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

    public boolean checkOperandTypes(SqlCallBinding callBinding,
        boolean throwOnFailure) {
      return checkSingleOperandType(callBinding, callBinding.operand(0), 0,
          throwOnFailure);
    }

    public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.of(1);
    }

    public String getAllowedSignatures(SqlOperator op, String opName) {
      return SqlUtil.getAliasedSignature(op, opName,
          ImmutableList.of("PERIOD (DATETIME, INTERVAL)",
              "PERIOD (DATETIME, DATETIME)"));
    }

    public boolean isOptional(int i) {
      return false;
    }

    public Consistency getConsistency() {
      return Consistency.NONE;
    }
  }
}

// End OperandTypes.java
