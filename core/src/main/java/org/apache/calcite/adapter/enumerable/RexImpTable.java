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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.OptimizeShuttle;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ImplementableAggFunction;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction;
import org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.calcite.linq4j.tree.ExpressionType.Add;
import static org.apache.calcite.linq4j.tree.ExpressionType.AndAlso;
import static org.apache.calcite.linq4j.tree.ExpressionType.Divide;
import static org.apache.calcite.linq4j.tree.ExpressionType.Equal;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Multiply;
import static org.apache.calcite.linq4j.tree.ExpressionType.Negate;
import static org.apache.calcite.linq4j.tree.ExpressionType.Not;
import static org.apache.calcite.linq4j.tree.ExpressionType.NotEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.OrElse;
import static org.apache.calcite.linq4j.tree.ExpressionType.Subtract;
import static org.apache.calcite.linq4j.tree.ExpressionType.UnaryPlus;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DIFFERENCE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_DEPTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_KEYS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_PRETTY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_REMOVE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_STORAGE_SIZE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_TYPE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LEFT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MD5;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MONTHNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REPEAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RIGHT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SHA1;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SOUNDEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ABS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ACOS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ANY_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASCII;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ASIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN2;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BIT_AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BIT_OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CARDINALITY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COALESCE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COLLECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CONCAT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_CATALOG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_PATH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_ROLE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DATETIME_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEFAULT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DEGREES;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DENSE_RANK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ELEMENT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FINAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FIRST_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FUSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING_ID;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUP_ID;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.INITCAP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_A_SET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_A_SET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_EMPTY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_SCALAR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_ARRAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_ARRAYAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_EXISTS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_OBJECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_OBJECTAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_QUERY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_ANY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_EXPRESSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_DAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LEAD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LISTAGG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOG10;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MAX;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MEMBER_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_EXCEPT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_INTERSECT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_UNION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTISET_UNION_DISTINCT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NEXT_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTH_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTILE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OVERLAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PREV;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RADIANS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RAND_INTEGER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.RANK;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REGR_COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REINTERPRET;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW_NUMBER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SINGLE_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SLICE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.STRUCT_ACCESS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM0;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SYSTEM_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRIM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRUNCATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.USER;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
  public static final ConstantExpression NULL_EXPR =
      Expressions.constant(null);
  public static final ConstantExpression FALSE_EXPR =
      Expressions.constant(false);
  public static final ConstantExpression TRUE_EXPR =
      Expressions.constant(true);
  public static final ConstantExpression COMMA_EXPR =
      Expressions.constant(",");
  public static final MemberExpression BOXED_FALSE_EXPR =
      Expressions.field(null, Boolean.class, "FALSE");
  public static final MemberExpression BOXED_TRUE_EXPR =
      Expressions.field(null, Boolean.class, "TRUE");

  private final Map<SqlOperator, CallImplementor> map = new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends AggImplementor>> aggMap =
      new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends WinAggImplementor>> winAggMap =
      new HashMap<>();

  RexImpTable() {
    defineMethod(ROW, BuiltInMethod.ARRAY.method, NullPolicy.ANY);
    defineMethod(UPPER, BuiltInMethod.UPPER.method, NullPolicy.STRICT);
    defineMethod(LOWER, BuiltInMethod.LOWER.method, NullPolicy.STRICT);
    defineMethod(INITCAP,  BuiltInMethod.INITCAP.method, NullPolicy.STRICT);
    defineMethod(TO_BASE64, BuiltInMethod.TO_BASE64.method, NullPolicy.STRICT);
    defineMethod(FROM_BASE64, BuiltInMethod.FROM_BASE64.method, NullPolicy.STRICT);
    defineMethod(MD5, BuiltInMethod.MD5.method, NullPolicy.STRICT);
    defineMethod(SHA1, BuiltInMethod.SHA1.method, NullPolicy.STRICT);
    defineMethod(SUBSTRING, BuiltInMethod.SUBSTRING.method, NullPolicy.STRICT);
    defineMethod(LEFT, BuiltInMethod.LEFT.method, NullPolicy.ANY);
    defineMethod(RIGHT, BuiltInMethod.RIGHT.method, NullPolicy.ANY);
    defineMethod(REPLACE, BuiltInMethod.REPLACE.method, NullPolicy.STRICT);
    defineMethod(TRANSLATE3, BuiltInMethod.TRANSLATE3.method, NullPolicy.STRICT);
    defineMethod(CHR, "chr", NullPolicy.STRICT);
    defineMethod(CHARACTER_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
        NullPolicy.STRICT);
    defineMethod(CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
        NullPolicy.STRICT);
    defineMethod(CONCAT, BuiltInMethod.STRING_CONCAT.method,
        NullPolicy.STRICT);
    defineMethod(OVERLAY, BuiltInMethod.OVERLAY.method, NullPolicy.STRICT);
    defineMethod(POSITION, BuiltInMethod.POSITION.method, NullPolicy.STRICT);
    defineMethod(ASCII, BuiltInMethod.ASCII.method, NullPolicy.STRICT);
    defineMethod(REPEAT, BuiltInMethod.REPEAT.method, NullPolicy.STRICT);
    defineMethod(SPACE, BuiltInMethod.SPACE.method, NullPolicy.STRICT);
    defineMethod(SOUNDEX, BuiltInMethod.SOUNDEX.method, NullPolicy.STRICT);
    defineMethod(DIFFERENCE, BuiltInMethod.DIFFERENCE.method, NullPolicy.STRICT);
    defineMethod(REVERSE, BuiltInMethod.REVERSE.method, NullPolicy.STRICT);

    final TrimImplementor trimImplementor = new TrimImplementor();
    defineImplementor(TRIM, NullPolicy.STRICT, trimImplementor, false);

    // logical
    defineBinary(AND, AndAlso, NullPolicy.AND, null);
    defineBinary(OR, OrElse, NullPolicy.OR, null);
    defineUnary(NOT, Not, NullPolicy.NOT);

    // comparisons
    defineBinary(LESS_THAN, LessThan, NullPolicy.STRICT, "lt");
    defineBinary(LESS_THAN_OR_EQUAL, LessThanOrEqual, NullPolicy.STRICT, "le");
    defineBinary(GREATER_THAN, GreaterThan, NullPolicy.STRICT, "gt");
    defineBinary(GREATER_THAN_OR_EQUAL, GreaterThanOrEqual, NullPolicy.STRICT,
        "ge");
    defineBinary(EQUALS, Equal, NullPolicy.STRICT, "eq");
    defineBinary(NOT_EQUALS, NotEqual, NullPolicy.STRICT, "ne");

    // arithmetic
    defineBinary(PLUS, Add, NullPolicy.STRICT, "plus");
    defineBinary(MINUS, Subtract, NullPolicy.STRICT, "minus");
    defineBinary(MULTIPLY, Multiply, NullPolicy.STRICT, "multiply");
    defineBinary(DIVIDE, Divide, NullPolicy.STRICT, "divide");
    defineBinary(DIVIDE_INTEGER, Divide, NullPolicy.STRICT, "divide");
    defineUnary(UNARY_MINUS, Negate, NullPolicy.STRICT);
    defineUnary(UNARY_PLUS, UnaryPlus, NullPolicy.STRICT);

    defineMethod(MOD, "mod", NullPolicy.STRICT);
    defineMethod(EXP, "exp", NullPolicy.STRICT);
    defineMethod(POWER, "power", NullPolicy.STRICT);
    defineMethod(LN, "ln", NullPolicy.STRICT);
    defineMethod(LOG10, "log10", NullPolicy.STRICT);
    defineMethod(ABS, "abs", NullPolicy.STRICT);

    defineImplementor(RAND, NullPolicy.STRICT,
        new NotNullImplementor() {
          final NotNullImplementor[] implementors = {
              new ReflectiveCallNotNullImplementor(BuiltInMethod.RAND.method),
              new ReflectiveCallNotNullImplementor(BuiltInMethod.RAND_SEED.method)
          };
          public Expression implement(RexToLixTranslator translator,
              RexCall call, List<Expression> translatedOperands) {
            return implementors[call.getOperands().size()]
                .implement(translator, call, translatedOperands);
          }
        }, false);
    defineImplementor(RAND_INTEGER, NullPolicy.STRICT,
        new NotNullImplementor() {
          final NotNullImplementor[] implementors = {
              null,
              new ReflectiveCallNotNullImplementor(
                BuiltInMethod.RAND_INTEGER.method),
              new ReflectiveCallNotNullImplementor(
                BuiltInMethod.RAND_INTEGER_SEED.method)
          };
          public Expression implement(RexToLixTranslator translator,
              RexCall call, List<Expression> translatedOperands) {
            return implementors[call.getOperands().size()]
                .implement(translator, call, translatedOperands);
          }
        }, false);

    defineMethod(ACOS, "acos", NullPolicy.STRICT);
    defineMethod(ASIN, "asin", NullPolicy.STRICT);
    defineMethod(ATAN, "atan", NullPolicy.STRICT);
    defineMethod(ATAN2, "atan2", NullPolicy.STRICT);
    defineMethod(COS, "cos", NullPolicy.STRICT);
    defineMethod(COT, "cot", NullPolicy.STRICT);
    defineMethod(DEGREES, "degrees", NullPolicy.STRICT);
    defineMethod(RADIANS, "radians", NullPolicy.STRICT);
    defineMethod(ROUND, "sround", NullPolicy.STRICT);
    defineMethod(SIGN, "sign", NullPolicy.STRICT);
    defineMethod(SIN, "sin", NullPolicy.STRICT);
    defineMethod(TAN, "tan", NullPolicy.STRICT);
    defineMethod(TRUNCATE, "struncate", NullPolicy.STRICT);

    map.put(PI, (translator, call, nullAs) -> Expressions.constant(Math.PI));

    // datetime
    defineImplementor(DATETIME_PLUS, NullPolicy.STRICT,
        new DatetimeArithmeticImplementor(), false);
    defineImplementor(MINUS_DATE, NullPolicy.STRICT,
        new DatetimeArithmeticImplementor(), false);
    defineImplementor(EXTRACT, NullPolicy.STRICT,
        new ExtractImplementor(), false);
    defineImplementor(FLOOR, NullPolicy.STRICT,
        new FloorImplementor(BuiltInMethod.FLOOR.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
            BuiltInMethod.UNIX_DATE_FLOOR.method), false);
    defineImplementor(CEIL, NullPolicy.STRICT,
        new FloorImplementor(BuiltInMethod.CEIL.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
            BuiltInMethod.UNIX_DATE_CEIL.method), false);

    defineMethod(LAST_DAY, "lastDay", NullPolicy.STRICT);
    defineImplementor(DAYNAME, NullPolicy.STRICT,
        new PeriodNameImplementor("dayName",
            BuiltInMethod.DAYNAME_WITH_TIMESTAMP,
            BuiltInMethod.DAYNAME_WITH_DATE), false);
    defineImplementor(MONTHNAME, NullPolicy.STRICT,
        new PeriodNameImplementor("monthName",
            BuiltInMethod.MONTHNAME_WITH_TIMESTAMP,
            BuiltInMethod.MONTHNAME_WITH_DATE), false);

    map.put(IS_NULL, new IsXxxImplementor(null, false));
    map.put(IS_NOT_NULL, new IsXxxImplementor(null, true));
    map.put(IS_TRUE, new IsXxxImplementor(true, false));
    map.put(IS_NOT_TRUE, new IsXxxImplementor(true, true));
    map.put(IS_FALSE, new IsXxxImplementor(false, false));
    map.put(IS_NOT_FALSE, new IsXxxImplementor(false, true));

    // LIKE and SIMILAR
    final MethodImplementor likeImplementor =
        new MethodImplementor(BuiltInMethod.LIKE.method);
    defineImplementor(LIKE, NullPolicy.STRICT, likeImplementor, false);
    defineImplementor(NOT_LIKE, NullPolicy.STRICT,
        NotImplementor.of(likeImplementor), false);
    final MethodImplementor similarImplementor =
        new MethodImplementor(BuiltInMethod.SIMILAR.method);
    defineImplementor(SIMILAR_TO, NullPolicy.STRICT, similarImplementor, false);
    defineImplementor(NOT_SIMILAR_TO, NullPolicy.STRICT,
        NotImplementor.of(similarImplementor), false);

    // POSIX REGEX
    final MethodImplementor posixRegexImplementor =
        new MethodImplementor(BuiltInMethod.POSIX_REGEX.method);
    defineImplementor(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE, NullPolicy.STRICT,
        posixRegexImplementor, false);
    defineImplementor(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE, NullPolicy.STRICT,
        posixRegexImplementor, false);
    defineImplementor(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE, NullPolicy.STRICT,
        NotImplementor.of(posixRegexImplementor), false);
    defineImplementor(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE, NullPolicy.STRICT,
        NotImplementor.of(posixRegexImplementor), false);
    defineImplementor(REGEXP_REPLACE, NullPolicy.STRICT,
        new NotNullImplementor() {
          final NotNullImplementor[] implementors = {
              new ReflectiveCallNotNullImplementor(
                  BuiltInMethod.REGEXP_REPLACE3.method),
              new ReflectiveCallNotNullImplementor(
                  BuiltInMethod.REGEXP_REPLACE4.method),
              new ReflectiveCallNotNullImplementor(
                  BuiltInMethod.REGEXP_REPLACE5.method),
              new ReflectiveCallNotNullImplementor(
                  BuiltInMethod.REGEXP_REPLACE6.method)
          };
          public Expression implement(RexToLixTranslator translator, RexCall call,
              List<Expression> translatedOperands) {
            return implementors[call.getOperands().size() - 3]
                .implement(translator, call, translatedOperands);
          }
        }, false);

    // Multisets & arrays
    defineMethod(CARDINALITY, BuiltInMethod.COLLECTION_SIZE.method,
        NullPolicy.STRICT);
    defineMethod(SLICE, BuiltInMethod.SLICE.method, NullPolicy.NONE);
    defineMethod(ELEMENT, BuiltInMethod.ELEMENT.method, NullPolicy.STRICT);
    defineMethod(STRUCT_ACCESS, BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY);
    defineMethod(MEMBER_OF, BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE);
    final MethodImplementor isEmptyImplementor =
        new MethodImplementor(BuiltInMethod.IS_EMPTY.method);
    defineImplementor(IS_EMPTY, NullPolicy.NONE, isEmptyImplementor, false);
    defineImplementor(IS_NOT_EMPTY, NullPolicy.NONE,
        NotImplementor.of(isEmptyImplementor), false);
    final MethodImplementor isASetImplementor =
        new MethodImplementor(BuiltInMethod.IS_A_SET.method);
    defineImplementor(IS_A_SET, NullPolicy.NONE, isASetImplementor, false);
    defineImplementor(IS_NOT_A_SET, NullPolicy.NONE,
        NotImplementor.of(isASetImplementor), false);
    defineMethod(MULTISET_INTERSECT_DISTINCT,
        BuiltInMethod.MULTISET_INTERSECT_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_INTERSECT,
        BuiltInMethod.MULTISET_INTERSECT_ALL.method, NullPolicy.NONE);
    defineMethod(MULTISET_EXCEPT_DISTINCT,
        BuiltInMethod.MULTISET_EXCEPT_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_EXCEPT, BuiltInMethod.MULTISET_EXCEPT_ALL.method, NullPolicy.NONE);
    defineMethod(MULTISET_UNION_DISTINCT,
        BuiltInMethod.MULTISET_UNION_DISTINCT.method, NullPolicy.NONE);
    defineMethod(MULTISET_UNION, BuiltInMethod.MULTISET_UNION_ALL.method, NullPolicy.NONE);
    final MethodImplementor subMultisetImplementor =
        new MethodImplementor(BuiltInMethod.SUBMULTISET_OF.method);
    defineImplementor(SUBMULTISET_OF, NullPolicy.NONE, subMultisetImplementor, false);
    defineImplementor(NOT_SUBMULTISET_OF,
        NullPolicy.NONE, NotImplementor.of(subMultisetImplementor), false);

    map.put(CASE, new CaseImplementor());
    map.put(COALESCE, new CoalesceImplementor());
    map.put(CAST, new CastOptimizedImplementor());

    defineImplementor(REINTERPRET, NullPolicy.STRICT,
        new ReinterpretImplementor(), false);

    final CallImplementor value = new ValueConstructorImplementor();
    map.put(MAP_VALUE_CONSTRUCTOR, value);
    map.put(ARRAY_VALUE_CONSTRUCTOR, value);
    map.put(ITEM, new ItemImplementor());

    map.put(DEFAULT, (translator, call, nullAs) -> Expressions.constant(null));

    // Sequences
    defineMethod(CURRENT_VALUE, BuiltInMethod.SEQUENCE_CURRENT_VALUE.method, NullPolicy.STRICT);
    defineMethod(NEXT_VALUE, BuiltInMethod.SEQUENCE_NEXT_VALUE.method, NullPolicy.STRICT);

    // Json Operators
    defineMethod(JSON_VALUE_EXPRESSION,
        BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT);
    defineMethod(JSON_EXISTS, BuiltInMethod.JSON_EXISTS.method, NullPolicy.ARG0);
    defineMethod(JSON_VALUE_ANY, BuiltInMethod.JSON_VALUE_ANY.method, NullPolicy.ARG0);
    defineMethod(JSON_QUERY, BuiltInMethod.JSON_QUERY.method, NullPolicy.ARG0);
    defineMethod(JSON_TYPE, BuiltInMethod.JSON_TYPE.method, NullPolicy.ARG0);
    defineMethod(JSON_DEPTH, BuiltInMethod.JSON_DEPTH.method, NullPolicy.ARG0);
    defineMethod(JSON_KEYS, BuiltInMethod.JSON_KEYS.method, NullPolicy.ARG0);
    defineMethod(JSON_PRETTY, BuiltInMethod.JSON_PRETTY.method, NullPolicy.ARG0);
    defineMethod(JSON_LENGTH, BuiltInMethod.JSON_LENGTH.method, NullPolicy.ARG0);
    defineMethod(JSON_REMOVE, BuiltInMethod.JSON_REMOVE.method, NullPolicy.ARG0);
    defineMethod(JSON_STORAGE_SIZE, BuiltInMethod.JSON_STORAGE_SIZE.method, NullPolicy.ARG0);
    defineMethod(JSON_OBJECT, BuiltInMethod.JSON_OBJECT.method, NullPolicy.NONE);
    defineMethod(JSON_ARRAY, BuiltInMethod.JSON_ARRAY.method, NullPolicy.NONE);
    aggMap.put(JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
        JsonObjectAggImplementor
            .supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method));
    aggMap.put(JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
        JsonObjectAggImplementor
            .supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method));
    aggMap.put(JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
        JsonArrayAggImplementor
            .supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method));
    aggMap.put(JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
        JsonArrayAggImplementor
            .supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method));
    defineImplementor(IS_JSON_VALUE, NullPolicy.NONE,
        new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method), false);
    defineImplementor(IS_JSON_OBJECT, NullPolicy.NONE,
        new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method), false);
    defineImplementor(IS_JSON_ARRAY, NullPolicy.NONE,
        new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method), false);
    defineImplementor(IS_JSON_SCALAR, NullPolicy.NONE,
        new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method), false);
    defineImplementor(IS_NOT_JSON_VALUE, NullPolicy.NONE,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method)), false);
    defineImplementor(IS_NOT_JSON_OBJECT, NullPolicy.NONE,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method)), false);
    defineImplementor(IS_NOT_JSON_ARRAY, NullPolicy.NONE,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method)), false);
    defineImplementor(IS_NOT_JSON_SCALAR, NullPolicy.NONE,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method)), false);

    // System functions
    final SystemFunctionImplementor systemFunctionImplementor =
        new SystemFunctionImplementor();
    map.put(USER, systemFunctionImplementor);
    map.put(CURRENT_USER, systemFunctionImplementor);
    map.put(SESSION_USER, systemFunctionImplementor);
    map.put(SYSTEM_USER, systemFunctionImplementor);
    map.put(CURRENT_PATH, systemFunctionImplementor);
    map.put(CURRENT_ROLE, systemFunctionImplementor);
    map.put(CURRENT_CATALOG, systemFunctionImplementor);

    // Current time functions
    map.put(CURRENT_TIME, systemFunctionImplementor);
    map.put(CURRENT_TIMESTAMP, systemFunctionImplementor);
    map.put(CURRENT_DATE, systemFunctionImplementor);
    map.put(LOCALTIME, systemFunctionImplementor);
    map.put(LOCALTIMESTAMP, systemFunctionImplementor);

    aggMap.put(COUNT, constructorSupplier(CountImplementor.class));
    aggMap.put(REGR_COUNT, constructorSupplier(CountImplementor.class));
    aggMap.put(SUM0, constructorSupplier(SumImplementor.class));
    aggMap.put(SUM, constructorSupplier(SumImplementor.class));
    Supplier<MinMaxImplementor> minMax =
        constructorSupplier(MinMaxImplementor.class);
    aggMap.put(MIN, minMax);
    aggMap.put(MAX, minMax);
    aggMap.put(ANY_VALUE, minMax);
    final Supplier<BitOpImplementor> bitop = constructorSupplier(BitOpImplementor.class);
    aggMap.put(BIT_AND, bitop);
    aggMap.put(BIT_OR, bitop);
    aggMap.put(SINGLE_VALUE, constructorSupplier(SingleValueImplementor.class));
    aggMap.put(COLLECT, constructorSupplier(CollectImplementor.class));
    aggMap.put(LISTAGG, constructorSupplier(ListaggImplementor.class));
    aggMap.put(FUSION, constructorSupplier(FusionImplementor.class));
    final Supplier<GroupingImplementor> grouping =
        constructorSupplier(GroupingImplementor.class);
    aggMap.put(GROUPING, grouping);
    aggMap.put(GROUP_ID, grouping);
    aggMap.put(GROUPING_ID, grouping);
    winAggMap.put(RANK, constructorSupplier(RankImplementor.class));
    winAggMap.put(DENSE_RANK, constructorSupplier(DenseRankImplementor.class));
    winAggMap.put(ROW_NUMBER, constructorSupplier(RowNumberImplementor.class));
    winAggMap.put(FIRST_VALUE,
        constructorSupplier(FirstValueImplementor.class));
    winAggMap.put(NTH_VALUE, constructorSupplier(NthValueImplementor.class));
    winAggMap.put(LAST_VALUE, constructorSupplier(LastValueImplementor.class));
    winAggMap.put(LEAD, constructorSupplier(LeadImplementor.class));
    winAggMap.put(LAG, constructorSupplier(LagImplementor.class));
    winAggMap.put(NTILE, constructorSupplier(NtileImplementor.class));
    winAggMap.put(COUNT, constructorSupplier(CountWinImplementor.class));
    winAggMap.put(REGR_COUNT, constructorSupplier(CountWinImplementor.class));

    // Functions for MATCH_RECOGNIZE
    defineMethod(FINAL, "abs", NullPolicy.ANY);

    map.put(PREV, (translator, call, nullAs) -> {
      final RexNode node = call.getOperands().get(0);
      final RexNode offset = call.getOperands().get(1);
      final Expression offs = Expressions.multiply(translator.translate(offset),
          Expressions.constant(-1));
      ((EnumerableMatch.PrevInputGetter) translator.inputGetter).setOffset(offs);
      return translator.translate(node, nullAs);
    });
  }

  private <T> Supplier<T> constructorSupplier(Class<T> klass) {
    final Constructor<T> constructor;
    try {
      constructor = klass.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          klass + " should implement zero arguments constructor");
    }
    return () -> {
      try {
        return constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalStateException(
            "Error while creating aggregate implementor " + constructor, e);
      }
    };
  }

  private void defineImplementor(
      SqlOperator operator,
      NullPolicy nullPolicy,
      NotNullImplementor implementor,
      boolean harmonize) {
    CallImplementor callImplementor =
        createImplementor(implementor, nullPolicy, harmonize);
    map.put(operator, callImplementor);
  }

  private static RexCall call2(
      boolean harmonize,
      RexToLixTranslator translator,
      RexCall call) {
    if (!harmonize) {
      return call;
    }
    final List<RexNode> operands2 =
        harmonize(translator, call.getOperands());
    if (operands2.equals(call.getOperands())) {
      return call;
    }
    return call.clone(call.getType(), operands2);
  }

  public static CallImplementor createImplementor(
      final NotNullImplementor implementor,
      final NullPolicy nullPolicy,
      final boolean harmonize) {
    switch (nullPolicy) {
    case ANY:
    case STRICT:
    case SEMI_STRICT:
    case ARG0:
      return (translator, call, nullAs) -> implementNullSemantics0(
          translator, call, nullAs, nullPolicy, harmonize,
          implementor);
    case AND:
/* TODO:
            if (nullAs == NullAs.FALSE) {
                nullPolicy2 = NullPolicy.ANY;
            }
*/
      // If any of the arguments are false, result is false;
      // else if any arguments are null, result is null;
      // else true.
      //
      // b0 == null ? (b1 == null || b1 ? null : Boolean.FALSE)
      //   : b0 ? b1
      //   : Boolean.FALSE;
      return (translator, call, nullAs) -> {
        assert call.getOperator() == AND
            : "AND null semantics is supported only for AND operator. Actual operator is "
            + String.valueOf(call.getOperator());
        final RexCall call2 = call2(false, translator, call);
        switch (nullAs) {
        case NOT_POSSIBLE:
          // This doesn't mean that none of the arguments might be null, ex: (s and s is not null)
          nullAs = NullAs.TRUE;
          // fallthru
        case TRUE:
          // AND call should return false iff has FALSEs,
          // thus if we convert nulls to true then no harm is made
        case FALSE:
          // AND call should return false iff has FALSEs or has NULLs,
          // thus if we convert nulls to false, no harm is made
          final List<Expression> expressions =
              translator.translateList(call2.getOperands(), nullAs);
          return Expressions.foldAnd(expressions);
        case NULL:
        case IS_NULL:
        case IS_NOT_NULL:
          final List<Expression> nullAsTrue =
              translator.translateList(call2.getOperands(), NullAs.TRUE);
          final List<Expression> nullAsIsNull =
              translator.translateList(call2.getOperands(), NullAs.IS_NULL);
          Expression hasFalse =
              Expressions.not(Expressions.foldAnd(nullAsTrue));
          Expression hasNull = Expressions.foldOr(nullAsIsNull);
          return nullAs.handle(
              Expressions.condition(hasFalse, BOXED_FALSE_EXPR,
                  Expressions.condition(hasNull, NULL_EXPR, BOXED_TRUE_EXPR)));
        default:
          throw new IllegalArgumentException(
              "Unknown nullAs when implementing AND: " + nullAs);
        }
      };
    case OR:
      // If any of the arguments are true, result is true;
      // else if any arguments are null, result is null;
      // else false.
      //
      // b0 == null ? (b1 == null || !b1 ? null : Boolean.TRUE)
      //   : !b0 ? b1
      //   : Boolean.TRUE;
      return (translator, call, nullAs) -> {
        assert call.getOperator() == OR
            : "OR null semantics is supported only for OR operator. Actual operator is "
            + String.valueOf(call.getOperator());
        final RexCall call2 = call2(harmonize, translator, call);
        switch (nullAs) {
        case NOT_POSSIBLE:
          // This doesn't mean that none of the arguments might be null, ex: (s or s is null)
          nullAs = NullAs.FALSE;
          // fallthru
        case TRUE:
          // This should return false iff all arguments are FALSE,
          // thus we convert nulls to TRUE and foldOr
        case FALSE:
          // This should return true iff has TRUE arguments,
          // thus we convert nulls to FALSE and foldOr
          final List<Expression> expressions =
              translator.translateList(call2.getOperands(), nullAs);
          return Expressions.foldOr(expressions);
        case NULL:
        case IS_NULL:
        case IS_NOT_NULL:
          final List<Expression> nullAsFalse =
              translator.translateList(call2.getOperands(), NullAs.FALSE);
          final List<Expression> nullAsIsNull =
              translator.translateList(call2.getOperands(), NullAs.IS_NULL);
          Expression hasTrue = Expressions.foldOr(nullAsFalse);
          Expression hasNull = Expressions.foldOr(nullAsIsNull);
          Expression result = nullAs.handle(
              Expressions.condition(hasTrue, BOXED_TRUE_EXPR,
                  Expressions.condition(hasNull, NULL_EXPR, BOXED_FALSE_EXPR)));
          return result;
        default:
          throw new IllegalArgumentException(
              "Unknown nullAs when implementing OR: " + nullAs);
        }
      };
    case NOT:
      // If any of the arguments are false, result is true;
      // else if any arguments are null, result is null;
      // else false.
      return new CallImplementor() {
        public Expression implement(RexToLixTranslator translator, RexCall call,
            NullAs nullAs) {
          switch (nullAs) {
          case NULL:
            return Expressions.call(BuiltInMethod.NOT.method,
                translator.translateList(call.getOperands(), nullAs));
          default:
            return Expressions.not(
                translator.translate(call.getOperands().get(0),
                    negate(nullAs)));
          }
        }

        private NullAs negate(NullAs nullAs) {
          switch (nullAs) {
          case FALSE:
            return NullAs.TRUE;
          case TRUE:
            return NullAs.FALSE;
          case IS_NULL:
            return NullAs.IS_NOT_NULL;
          case IS_NOT_NULL:
            return NullAs.IS_NULL;
          default:
            return nullAs;
          }
        }
      };
    case NONE:
      return (translator, call, nullAs) -> {
        final RexCall call2 = call2(false, translator, call);
        return implementCall(
            translator, call2, implementor, nullAs);
      };
    default:
      throw new AssertionError(nullPolicy);
    }
  }

  private void defineMethod(
      SqlOperator operator, String functionName, NullPolicy nullPolicy) {
    defineImplementor(
        operator,
        nullPolicy,
        new MethodNameImplementor(functionName),
        false);
  }

  private void defineMethod(
      SqlOperator operator, Method method, NullPolicy nullPolicy) {
    defineImplementor(
        operator, nullPolicy, new MethodImplementor(method), false);
  }

  private void defineMethodReflective(
      SqlOperator operator, Method method, NullPolicy nullPolicy) {
    defineImplementor(
        operator, nullPolicy, new ReflectiveCallNotNullImplementor(method),
        false);
  }

  private void defineUnary(
      SqlOperator operator, ExpressionType expressionType,
      NullPolicy nullPolicy) {
    defineImplementor(
        operator,
        nullPolicy,
        new UnaryImplementor(expressionType), false);
  }

  private void defineBinary(
      SqlOperator operator,
      ExpressionType expressionType,
      NullPolicy nullPolicy,
      String backupMethodName) {
    defineImplementor(
        operator,
        nullPolicy,
        new BinaryImplementor(expressionType, backupMethodName),
        true);
  }

  public static final RexImpTable INSTANCE = new RexImpTable();

  public CallImplementor get(final SqlOperator operator) {
    if (operator instanceof SqlUserDefinedFunction) {
      org.apache.calcite.schema.Function udf =
          ((SqlUserDefinedFunction) operator).getFunction();
      if (!(udf instanceof ImplementableFunction)) {
        throw new IllegalStateException("User defined function " + operator
            + " must implement ImplementableFunction");
      }
      return ((ImplementableFunction) udf).getImplementor();
    }
    return map.get(operator);
  }

  public AggImplementor get(final SqlAggFunction aggregation,
      boolean forWindowAggregate) {
    if (aggregation instanceof SqlUserDefinedAggFunction) {
      final SqlUserDefinedAggFunction udaf =
          (SqlUserDefinedAggFunction) aggregation;
      if (!(udaf.function instanceof ImplementableAggFunction)) {
        throw new IllegalStateException("User defined aggregation "
            + aggregation + " must implement ImplementableAggFunction");
      }
      return ((ImplementableAggFunction) udaf.function)
          .getImplementor(forWindowAggregate);
    }
    if (forWindowAggregate) {
      Supplier<? extends WinAggImplementor> winAgg =
          winAggMap.get(aggregation);
      if (winAgg != null) {
        return winAgg.get();
      }
      // Regular aggregates can be used in window context as well
    }

    Supplier<? extends AggImplementor> aggSupplier = aggMap.get(aggregation);
    if (aggSupplier == null) {
      return null;
    }

    return aggSupplier.get();
  }

  static Expression maybeNegate(boolean negate, Expression expression) {
    if (!negate) {
      return expression;
    } else {
      return Expressions.not(expression);
    }
  }

  static Expression optimize(Expression expression) {
    return expression.accept(new OptimizeShuttle());
  }

  static Expression optimize2(Expression operand, Expression expression) {
    if (Primitive.is(operand.getType())) {
      // Primitive values cannot be null
      return optimize(expression);
    } else {
      return optimize(
          Expressions.condition(
              Expressions.equal(
                  operand,
                  NULL_EXPR),
              NULL_EXPR,
              expression));
    }
  }

  private static boolean nullable(RexCall call, int i) {
    return call.getOperands().get(i).getType().isNullable();
  }

  /** Ensures that operands have identical type. */
  private static List<RexNode> harmonize(
      final RexToLixTranslator translator, final List<RexNode> operands) {
    int nullCount = 0;
    final List<RelDataType> types = new ArrayList<>();
    final RelDataTypeFactory typeFactory =
        translator.builder.getTypeFactory();
    for (RexNode operand : operands) {
      RelDataType type = operand.getType();
      type = toSql(typeFactory, type);
      if (translator.isNullable(operand)) {
        ++nullCount;
      } else {
        type = typeFactory.createTypeWithNullability(type, false);
      }
      types.add(type);
    }
    if (allSame(types)) {
      // Operands have the same nullability and type. Return them
      // unchanged.
      return operands;
    }
    final RelDataType type = typeFactory.leastRestrictive(types);
    if (type == null) {
      // There is no common type. Presumably this is a binary operator with
      // asymmetric arguments (e.g. interval / integer) which is not intended
      // to be harmonized.
      return operands;
    }
    assert (nullCount > 0) == type.isNullable();
    final List<RexNode> list = new ArrayList<>();
    for (RexNode operand : operands) {
      list.add(
          translator.builder.ensureType(type, operand, false));
    }
    return list;
  }

  private static RelDataType toSql(RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      final SqlTypeName typeName = type.getSqlTypeName();
      if (typeName != null && typeName != SqlTypeName.OTHER) {
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(typeName),
            type.isNullable());
      }
    }
    return type;
  }

  private static <E> boolean allSame(List<E> list) {
    E prev = null;
    for (E e : list) {
      if (prev != null && !prev.equals(e)) {
        return false;
      }
      prev = e;
    }
    return true;
  }

  private static Expression implementNullSemantics0(
      RexToLixTranslator translator,
      RexCall call,
      NullAs nullAs,
      NullPolicy nullPolicy,
      boolean harmonize,
      NotNullImplementor implementor) {
    switch (nullAs) {
    case IS_NOT_NULL:
      // If "f" is strict, then "f(a0, a1) IS NOT NULL" is
      // equivalent to "a0 IS NOT NULL AND a1 IS NOT NULL".
      switch (nullPolicy) {
      case STRICT:
        return Expressions.foldAnd(
            translator.translateList(
                call.getOperands(), nullAs));
      }
      break;
    case IS_NULL:
      // If "f" is strict, then "f(a0, a1) IS NULL" is
      // equivalent to "a0 IS NULL OR a1 IS NULL".
      switch (nullPolicy) {
      case STRICT:
        return Expressions.foldOr(
            translator.translateList(
                call.getOperands(), nullAs));
      }
      break;
    }
    final RexCall call2 = call2(harmonize, translator, call);
    try {
      return implementNullSemantics(
          translator, call2, nullAs, nullPolicy, implementor);
    } catch (RexToLixTranslator.AlwaysNull e) {
      switch (nullAs) {
      case NOT_POSSIBLE:
        throw e;
      case FALSE:
      case IS_NOT_NULL:
        return FALSE_EXPR;
      case TRUE:
      case IS_NULL:
        return TRUE_EXPR;
      default:
        return NULL_EXPR;
      }
    }
  }

  private static Expression implementNullSemantics(
      RexToLixTranslator translator,
      RexCall call,
      NullAs nullAs,
      NullPolicy nullPolicy,
      NotNullImplementor implementor) {
    final List<Expression> list = new ArrayList<>();
    final List<RexNode> conditionalOps =
        nullPolicy == NullPolicy.ARG0
            ? Collections.singletonList(call.getOperands().get(0))
            : call.getOperands();
    switch (nullAs) {
    case NULL:
    case IS_NULL:
    case IS_NOT_NULL:
      // v0 == null || v1 == null ? null : f(v0, v1)
      for (Ord<RexNode> operand : Ord.zip(conditionalOps)) {
        if (translator.isNullable(operand.e)) {
          list.add(
              translator.translate(
                  operand.e, NullAs.IS_NULL));
          translator = translator.setNullable(operand.e, false);
        }
      }
      final Expression box =
          Expressions.box(
              implementCall(translator, call, implementor, nullAs));
      final Expression ifTrue;
      switch (nullAs) {
      case NULL:
        ifTrue = Types.castIfNecessary(box.getType(), NULL_EXPR);
        break;
      case IS_NULL:
        ifTrue = TRUE_EXPR;
        break;
      case IS_NOT_NULL:
        ifTrue = FALSE_EXPR;
        break;
      default:
        throw new AssertionError();
      }
      return optimize(
          Expressions.condition(
              Expressions.foldOr(list),
              ifTrue,
              box));
    case FALSE:
      // v0 != null && v1 != null && f(v0, v1)
      for (Ord<RexNode> operand : Ord.zip(conditionalOps)) {
        if (translator.isNullable(operand.e)) {
          list.add(
              translator.translate(
                  operand.e, NullAs.IS_NOT_NULL));
          translator = translator.setNullable(operand.e, false);
        }
      }
      list.add(implementCall(translator, call, implementor, nullAs));
      return Expressions.foldAnd(list);
    case TRUE:
      // v0 == null || v1 == null || f(v0, v1)
      for (Ord<RexNode> operand : Ord.zip(conditionalOps)) {
        if (translator.isNullable(operand.e)) {
          list.add(
              translator.translate(
                  operand.e, NullAs.IS_NULL));
          translator = translator.setNullable(operand.e, false);
        }
      }
      list.add(implementCall(translator, call, implementor, nullAs));
      return Expressions.foldOr(list);
    case NOT_POSSIBLE:
      // Need to transmit to the implementor the fact that call cannot
      // return null. In particular, it should return a primitive (e.g.
      // int) rather than a box type (Integer).
      // The cases with setNullable above might not help since the same
      // RexNode can be referred via multiple ways: RexNode itself, RexLocalRef,
      // and may be others.
      final Map<RexNode, Boolean> nullable = new HashMap<>();
      switch (nullPolicy) {
      case STRICT:
        // The arguments should be not nullable if STRICT operator is computed
        // in nulls NOT_POSSIBLE mode
        for (RexNode arg : call.getOperands()) {
          if (translator.isNullable(arg) && !nullable.containsKey(arg)) {
            nullable.put(arg, false);
          }
        }
      }
      nullable.put(call, false);
      translator = translator.setNullable(nullable);
      // fall through
    default:
      return implementCall(translator, call, implementor, nullAs);
    }
  }

  private static Expression implementCall(
      final RexToLixTranslator translator,
      RexCall call,
      NotNullImplementor implementor,
      final NullAs nullAs) {
    List<Expression> translatedOperands =
        translator.translateList(call.getOperands());
    // Make sure the operands marked not null in the translator have all been
    // handled for nulls before being passed to the NotNullImplementor.
    if (nullAs == NullAs.NOT_POSSIBLE) {
      List<Expression> nullHandled = translatedOperands;
      for (int i = 0; i < translatedOperands.size(); i++) {
        RexNode arg = call.getOperands().get(i);
        Expression e = translatedOperands.get(i);
        if (!translator.isNullable(arg)) {
          if (nullHandled == translatedOperands) {
            nullHandled = new ArrayList<>(translatedOperands.subList(0, i));
          }
          nullHandled.add(translator.handleNull(e, nullAs));
        } else if (nullHandled != translatedOperands) {
          nullHandled.add(e);
        }
      }
      translatedOperands = nullHandled;
    }
    Expression result =
        implementor.implement(translator, call, translatedOperands);
    return translator.handleNull(result, nullAs);
  }

  /** Strategy what an operator should return if one of its
   * arguments is null. */
  public enum NullAs {
    /** The most common policy among the SQL built-in operators. If
     * one of the arguments is null, returns null. */
    NULL,

    /** If one of the arguments is null, the function returns
     * false. Example: {@code IS NOT NULL}. */
    FALSE,

    /** If one of the arguments is null, the function returns
     * true. Example: {@code IS NULL}. */
    TRUE,

    /** It is not possible for any of the arguments to be null.  If
     * the argument type is nullable, the enclosing code will already
     * have performed a not-null check. This may allow the operator
     * implementor to generate a more efficient implementation, for
     * example, by avoiding boxing or unboxing. */
    NOT_POSSIBLE,

    /** Return false if result is not null, true if result is null. */
    IS_NULL,

    /** Return true if result is not null, false if result is null. */
    IS_NOT_NULL;

    public static NullAs of(boolean nullable) {
      return nullable ? NULL : NOT_POSSIBLE;
    }

    /** Adapts an expression with "normal" result to one that adheres to
     * this particular policy. */
    public Expression handle(Expression x) {
      switch (Primitive.flavor(x.getType())) {
      case PRIMITIVE:
        // Expression cannot be null. We can skip any runtime checks.
        switch (this) {
        case NULL:
        case NOT_POSSIBLE:
        case FALSE:
        case TRUE:
          return x;
        case IS_NULL:
          return FALSE_EXPR;
        case IS_NOT_NULL:
          return TRUE_EXPR;
        default:
          throw new AssertionError();
        }
      case BOX:
        switch (this) {
        case NOT_POSSIBLE:
          return RexToLixTranslator.convert(
              x,
              Primitive.ofBox(x.getType()).primitiveClass);
        }
        // fall through
      }
      switch (this) {
      case NULL:
      case NOT_POSSIBLE:
        return x;
      case FALSE:
        return Expressions.call(
            BuiltInMethod.IS_TRUE.method,
            x);
      case TRUE:
        return Expressions.call(
            BuiltInMethod.IS_NOT_FALSE.method,
            x);
      case IS_NULL:
        return Expressions.equal(x, NULL_EXPR);
      case IS_NOT_NULL:
        return Expressions.notEqual(x, NULL_EXPR);
      default:
        throw new AssertionError();
      }
    }
  }

  static Expression getDefaultValue(Type type) {
    if (Primitive.is(type)) {
      Primitive p = Primitive.of(type);
      return Expressions.constant(p.defaultValue, type);
    }
    return Expressions.constant(null, type);
  }

  /** Multiplies an expression by a constant and divides by another constant,
   * optimizing appropriately.
   *
   * <p>For example, {@code multiplyDivide(e, 10, 1000)} returns
   * {@code e / 100}. */
  public static Expression multiplyDivide(Expression e, BigDecimal multiplier,
      BigDecimal divider) {
    if (multiplier.equals(BigDecimal.ONE)) {
      if (divider.equals(BigDecimal.ONE)) {
        return e;
      }
      return Expressions.divide(e,
          Expressions.constant(divider.intValueExact()));
    }
    final BigDecimal x =
        multiplier.divide(divider, RoundingMode.UNNECESSARY);
    switch (x.compareTo(BigDecimal.ONE)) {
    case 0:
      return e;
    case 1:
      return Expressions.multiply(e, Expressions.constant(x.intValueExact()));
    case -1:
      return multiplyDivide(e, BigDecimal.ONE, x);
    default:
      throw new AssertionError();
    }
  }

  /** Implementor for the {@code COUNT} aggregate function. */
  static class CountImplementor extends StrictAggImplementor {
    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      add.currentBlock().add(
          Expressions.statement(
              Expressions.postIncrementAssign(add.accumulator().get(0))));
    }
  }

  /** Implementor for the {@code COUNT} windowed aggregate function. */
  static class CountWinImplementor extends StrictWinAggImplementor {
    boolean justFrameRowCount;

    @Override public List<Type> getNotNullState(WinAggContext info) {
      boolean hasNullable = false;
      for (RelDataType type : info.parameterRelTypes()) {
        if (type.isNullable()) {
          hasNullable = true;
          break;
        }
      }
      if (!hasNullable) {
        justFrameRowCount = true;
        return Collections.emptyList();
      }
      return super.getNotNullState(info);
    }

    @Override public void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      if (justFrameRowCount) {
        return;
      }
      add.currentBlock().add(
          Expressions.statement(
              Expressions.postIncrementAssign(add.accumulator().get(0))));
    }

    @Override protected Expression implementNotNullResult(WinAggContext info,
        WinAggResultContext result) {
      if (justFrameRowCount) {
        return result.getFrameRowCount();
      }
      return super.implementNotNullResult(info, result);
    }
  }

  /** Implementor for the {@code SUM} windowed aggregate function. */
  static class SumImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Expression start = info.returnType() == BigDecimal.class
          ? Expressions.constant(BigDecimal.ZERO)
          : Expressions.constant(0);

      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), start)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression next;
      if (info.returnType() == BigDecimal.class) {
        next = Expressions.call(acc, "add", add.arguments().get(0));
      } else {
        next = Expressions.add(acc,
            Types.castIfNecessary(acc.type, add.arguments().get(0)));
      }
      accAdvance(add, acc, next);
    }

    @Override public Expression implementNotNullResult(AggContext info,
        AggResultContext result) {
      return super.implementNotNullResult(info, result);
    }
  }

  /** Implementor for the {@code MIN} and {@code MAX} aggregate functions. */
  static class MinMaxImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Expression acc = reset.accumulator().get(0);
      Primitive p = Primitive.of(acc.getType());
      boolean isMin = MIN == info.aggregation();
      Object inf = p == null ? null : (isMin ? p.max : p.min);
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc,
                  Expressions.constant(inf, acc.getType()))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression arg = add.arguments().get(0);
      SqlAggFunction aggregation = info.aggregation();
      final Method method = (aggregation == MIN
          ? BuiltInMethod.LESSER
          : BuiltInMethod.GREATER).method;
      Expression next = Expressions.call(
          method.getDeclaringClass(),
          method.getName(),
          acc,
          Expressions.unbox(arg));
      accAdvance(add, acc, next);
    }
  }

  /** Implementor for the {@code SINGLE_VALUE} aggregate function. */
  static class SingleValueImplementor implements AggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Arrays.asList(boolean.class, info.returnType());
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      List<Expression> acc = reset.accumulator();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0), Expressions.constant(false))));
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(1),
                  getDefaultValue(acc.get(1).getType()))));
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      List<Expression> acc = add.accumulator();
      Expression flag = acc.get(0);
      add.currentBlock().add(
          Expressions.ifThen(flag,
              Expressions.throw_(
                  Expressions.new_(IllegalStateException.class,
                      Expressions.constant("more than one value in agg "
                          + info.aggregation())))));
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(flag, Expressions.constant(true))));
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(1), add.arguments().get(0))));
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      return RexToLixTranslator.convert(result.accumulator().get(1),
          info.returnType());
    }
  }

  /** Implementor for the {@code COLLECT} aggregate function. */
  static class CollectImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      // acc[0] = new ArrayList();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      // acc[0].add(arg);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_ADD.method,
                  add.arguments().get(0))));
    }
  }

  /** Implementor for the {@code LISTAGG} aggregate function. */
  static class ListaggImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), NULL_EXPR)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      final Expression accValue = add.accumulator().get(0);
      final Expression arg0 = add.arguments().get(0);
      final Expression arg1 = add.arguments().size() == 2
          ? add.arguments().get(1) : COMMA_EXPR;
      final Expression result = Expressions.condition(
          Expressions.equal(NULL_EXPR, accValue),
          arg0,
          Expressions.call(BuiltInMethod.STRING_CONCAT.method, accValue,
              Expressions.call(BuiltInMethod.STRING_CONCAT.method, arg1, arg0)));

      add.currentBlock().add(Expressions.statement(Expressions.assign(accValue, result)));
    }
  }

  /** Implementor for the {@code FUSION} aggregate function. */
  static class FusionImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      // acc[0] = new ArrayList();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      // acc[0].add(arg);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_ADDALL.method,
                  add.arguments().get(0))));
    }
  }

  /** Implementor for the {@code BIT_AND} and {@code BIT_OR} aggregate function. */
  static class BitOpImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Object initValue = info.aggregation() == BIT_AND ? -1 : 0;
      Expression start = Expressions.constant(initValue, info.returnType());

      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), start)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression arg = add.arguments().get(0);
      SqlAggFunction aggregation = info.aggregation();
      final Method method = (aggregation == BIT_AND
          ? BuiltInMethod.BIT_AND
          : BuiltInMethod.BIT_OR).method;
      Expression next = Expressions.call(
          method.getDeclaringClass(),
          method.getName(),
          acc,
          Expressions.unbox(arg));
      accAdvance(add, acc, next);
    }
  }

  /** Implementor for the {@code GROUPING} aggregate function. */
  static class GroupingImplementor implements AggImplementor {
    public List<Type> getStateType(AggContext info) {
      return ImmutableList.of();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
    }

    public void implementAdd(AggContext info, AggAddContext add) {
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      final List<Integer> keys;
      switch (info.aggregation().kind) {
      case GROUPING: // "GROUPING(e, ...)", also "GROUPING_ID(e, ...)"
        keys = result.call().getArgList();
        break;
      case GROUP_ID: // "GROUP_ID()"
        // We don't implement GROUP_ID properly. In most circumstances, it
        // returns 0, so we always return 0. Logged
        // [CALCITE-1824] GROUP_ID returns wrong result
        keys = ImmutableIntList.of();
        break;
      default:
        throw new AssertionError();
      }
      Expression e = null;
      if (info.groupSets().size() > 1) {
        final List<Integer> keyOrdinals = info.keyOrdinals();
        long x = 1L << (keys.size() - 1);
        for (int k : keys) {
          final int i = keyOrdinals.indexOf(k);
          assert i >= 0;
          final Expression e2 =
              Expressions.condition(result.keyField(keyOrdinals.size() + i),
                  Expressions.constant(x),
                  Expressions.constant(0L));
          if (e == null) {
            e = e2;
          } else {
            e = Expressions.add(e, e2);
          }
          x >>= 1;
        }
      }
      return e != null ? e : Expressions.constant(0, info.returnType());
    }
  }

  /** Implementor for user-defined aggregate functions. */
  public static class UserDefinedAggReflectiveImplementor
      extends StrictAggImplementor {
    private final AggregateFunctionImpl afi;

    public UserDefinedAggReflectiveImplementor(AggregateFunctionImpl afi) {
      this.afi = afi;
    }

    @Override public List<Type> getNotNullState(AggContext info) {
      if (afi.isStatic) {
        return Collections.singletonList(afi.accumulatorType);
      }
      return Arrays.asList(afi.accumulatorType, afi.declaringClass);
    }

    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      List<Expression> acc = reset.accumulator();
      if (!afi.isStatic) {
        reset.currentBlock().add(
            Expressions.statement(
                Expressions.assign(acc.get(1),
                    Expressions.new_(afi.declaringClass))));
      }
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0),
                  Expressions.call(afi.isStatic
                      ? null
                      : acc.get(1), afi.initMethod))));
    }

    @Override protected void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      List<Expression> acc = add.accumulator();
      List<Expression> aggArgs = add.arguments();
      List<Expression> args = new ArrayList<>(aggArgs.size() + 1);
      args.add(acc.get(0));
      args.addAll(aggArgs);
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0),
                  Expressions.call(afi.isStatic ? null : acc.get(1), afi.addMethod,
                      args))));
    }

    @Override protected Expression implementNotNullResult(AggContext info,
        AggResultContext result) {
      List<Expression> acc = result.accumulator();
      return Expressions.call(
          afi.isStatic ? null : acc.get(1), afi.resultMethod, acc.get(0));
    }
  }

  /** Implementor for the {@code RANK} windowed aggregate function. */
  static class RankImplementor extends StrictWinAggImplementor {
    @Override protected void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      Expression acc = add.accumulator().get(0);
      // This is an example of the generated code
      if (false) {
        new Object() {
          int curentPosition; // position in for-win-agg-loop
          int startIndex;     // index of start of window
          Comparable[] rows;  // accessed via WinAggAddContext.compareRows
          {
            if (curentPosition > startIndex) {
              if (rows[curentPosition - 1].compareTo(rows[curentPosition])
                  > 0) {
                // update rank
              }
            }
          }
        };
      }
      BlockBuilder builder = add.nestBlock();
      add.currentBlock().add(
          Expressions.ifThen(
              Expressions.lessThan(
                  add.compareRows(
                      Expressions.subtract(add.currentPosition(),
                          Expressions.constant(1)),
                      add.currentPosition()),
                  Expressions.constant(0)),
              Expressions.statement(
                  Expressions.assign(acc, computeNewRank(acc, add)))));
      add.exitBlock();
      add.currentBlock().add(
          Expressions.ifThen(
              Expressions.greaterThan(add.currentPosition(),
                  add.startIndex()),
              builder.toBlock()));
    }

    protected Expression computeNewRank(Expression acc, WinAggAddContext add) {
      Expression pos = add.currentPosition();
      if (!add.startIndex().equals(Expressions.constant(0))) {
        // In general, currentPosition-startIndex should be used
        // However, rank/dense_rank does not allow preceding/following clause
        // so we always result in startIndex==0.
        pos = Expressions.subtract(pos, add.startIndex());
      }
      return pos;
    }

    @Override protected Expression implementNotNullResult(
        WinAggContext info, WinAggResultContext result) {
      // Rank is 1-based
      return Expressions.add(super.implementNotNullResult(info, result),
          Expressions.constant(1));
    }
  }

  /** Implementor for the {@code DENSE_RANK} windowed aggregate function. */
  static class DenseRankImplementor extends RankImplementor {
    @Override protected Expression computeNewRank(Expression acc,
        WinAggAddContext add) {
      return Expressions.add(acc, Expressions.constant(1));
    }
  }

  /** Implementor for the {@code FIRST_VALUE} and {@code LAST_VALUE}
   * windowed aggregate functions. */
  static class FirstLastValueImplementor implements WinAggImplementor {
    private final SeekType seekType;

    protected FirstLastValueImplementor(SeekType seekType) {
      this.seekType = seekType;
    }

    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return true;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      return Expressions.condition(winResult.hasRows(),
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), seekType))
              .translate(winResult.rexArguments().get(0), info.returnType()),
          getDefaultValue(info.returnType()));
    }
  }

  /** Implementor for the {@code FIRST_VALUE} windowed aggregate function. */
  static class FirstValueImplementor extends FirstLastValueImplementor {
    protected FirstValueImplementor() {
      super(SeekType.START);
    }
  }

  /** Implementor for the {@code LAST_VALUE} windowed aggregate function. */
  static class LastValueImplementor extends FirstLastValueImplementor {
    protected LastValueImplementor() {
      super(SeekType.END);
    }
  }

  /** Implementor for the {@code NTH_VALUE}
   * windowed aggregate function. */
  static class NthValueImplementor implements WinAggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return true;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      ParameterExpression res = Expressions.parameter(0, info.returnType(),
          result.currentBlock().newName("nth"));

      RexToLixTranslator currentRowTranslator =
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), SeekType.START));

      Expression dstIndex = winResult.computeIndex(
          Expressions.subtract(
              currentRowTranslator.translate(rexArgs.get(1), int.class),
              Expressions.constant(1)), SeekType.START);

      Expression rowInRange = winResult.rowInPartition(dstIndex);

      BlockBuilder thenBlock = result.nestBlock();
      Expression nthValue = winResult.rowTranslator(dstIndex)
          .translate(rexArgs.get(0), res.type);
      thenBlock.add(Expressions.statement(Expressions.assign(res, nthValue)));
      result.exitBlock();
      BlockStatement thenBranch = thenBlock.toBlock();

      Expression defaultValue = getDefaultValue(res.type);

      result.currentBlock().add(Expressions.declare(0, res, null));
      result.currentBlock().add(
          Expressions.ifThenElse(rowInRange, thenBranch,
              Expressions.statement(Expressions.assign(res, defaultValue))));
      return res;
    }
  }

  /** Implementor for the {@code LEAD} and {@code LAG} windowed
   * aggregate functions. */
  static class LeadLagImplementor implements WinAggImplementor {
    private final boolean isLead;

    protected LeadLagImplementor(boolean isLead) {
      this.isLead = isLead;
    }

    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return false;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      ParameterExpression res = Expressions.parameter(0, info.returnType(),
          result.currentBlock().newName(isLead ? "lead" : "lag"));

      Expression offset;
      RexToLixTranslator currentRowTranslator =
          winResult.rowTranslator(
              winResult.computeIndex(Expressions.constant(0), SeekType.SET));
      if (rexArgs.size() >= 2) {
        // lead(x, offset) or lead(x, offset, default)
        offset = currentRowTranslator.translate(
            rexArgs.get(1), int.class);
      } else {
        offset = Expressions.constant(1);
      }
      if (!isLead) {
        offset = Expressions.negate(offset);
      }
      Expression dstIndex = winResult.computeIndex(offset, SeekType.SET);

      Expression rowInRange = winResult.rowInPartition(dstIndex);

      BlockBuilder thenBlock = result.nestBlock();
      Expression lagResult = winResult.rowTranslator(dstIndex).translate(
          rexArgs.get(0), res.type);
      thenBlock.add(Expressions.statement(Expressions.assign(res, lagResult)));
      result.exitBlock();
      BlockStatement thenBranch = thenBlock.toBlock();

      Expression defaultValue = rexArgs.size() == 3
          ? currentRowTranslator.translate(rexArgs.get(2), res.type)
          : getDefaultValue(res.type);

      result.currentBlock().add(Expressions.declare(0, res, null));
      result.currentBlock().add(
          Expressions.ifThenElse(rowInRange, thenBranch,
              Expressions.statement(Expressions.assign(res, defaultValue))));
      return res;
    }
  }

  /** Implementor for the {@code LEAD} windowed aggregate function. */
  public static class LeadImplementor extends LeadLagImplementor {
    protected LeadImplementor() {
      super(true);
    }
  }

  /** Implementor for the {@code LAG} windowed aggregate function. */
  public static class LagImplementor extends LeadLagImplementor {
    protected LagImplementor() {
      super(false);
    }
  }

  /** Implementor for the {@code NTILE} windowed aggregate function. */
  static class NtileImplementor implements WinAggImplementor {
    public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    public boolean needCacheWhenFrameIntact() {
      return false;
    }

    public Expression implementResult(AggContext info,
        AggResultContext result) {
      WinAggResultContext winResult = (WinAggResultContext) result;

      List<RexNode> rexArgs = winResult.rexArguments();

      Expression tiles =
          winResult.rowTranslator(winResult.index()).translate(
              rexArgs.get(0), int.class);

      Expression ntile =
          Expressions.add(Expressions.constant(1),
              Expressions.divide(
                  Expressions.multiply(
                      tiles,
                      Expressions.subtract(
                          winResult.index(), winResult.startIndex())),
                  winResult.getPartitionRowCount()));

      return ntile;
    }
  }

  /** Implementor for the {@code ROW_NUMBER} windowed aggregate function. */
  static class RowNumberImplementor extends StrictWinAggImplementor {
    @Override public List<Type> getNotNullState(WinAggContext info) {
      return Collections.emptyList();
    }

    @Override protected void implementNotNullAdd(WinAggContext info,
        WinAggAddContext add) {
      // no op
    }

    @Override protected Expression implementNotNullResult(
        WinAggContext info, WinAggResultContext result) {
      // Window cannot be empty since ROWS/RANGE is not possible for ROW_NUMBER
      return Expressions.add(
          Expressions.subtract(result.index(), result.startIndex()),
          Expressions.constant(1));
    }
  }

  /** Implementor for the {@code JSON_OBJECTAGG} aggregate function. */
  static class JsonObjectAggImplementor implements AggImplementor {
    private final Method m;

    JsonObjectAggImplementor(Method m) {
      this.m = m;
    }

    static Supplier<JsonObjectAggImplementor> supplierFor(Method m) {
      return () -> new JsonObjectAggImplementor(m);
    }

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.singletonList(Map.class);
    }

    @Override public void implementReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(HashMap.class))));
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      final SqlJsonObjectAggAggFunction function =
          (SqlJsonObjectAggAggFunction) info.aggregation();
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(m,
                  Iterables.concat(
                      Collections.singletonList(add.accumulator().get(0)),
                      add.arguments(),
                      Collections.singletonList(
                          Expressions.constant(function.getNullClause()))))));
    }

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      return Expressions.call(BuiltInMethod.JSONIZE.method,
          result.accumulator().get(0));
    }
  }

  /** Implementor for the {@code JSON_ARRAYAGG} aggregate function. */
  static class JsonArrayAggImplementor implements AggImplementor {
    private final Method m;

    JsonArrayAggImplementor(Method m) {
      this.m = m;
    }

    static Supplier<JsonArrayAggImplementor> supplierFor(Method m) {
      return () -> new JsonArrayAggImplementor(m);
    }

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.singletonList(List.class);
    }

    @Override public void implementReset(AggContext info,
        AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.new_(ArrayList.class))));
    }

    @Override public void implementAdd(AggContext info,
        AggAddContext add) {
      final SqlJsonArrayAggAggFunction function =
          (SqlJsonArrayAggAggFunction) info.aggregation();
      add.currentBlock().add(
          Expressions.statement(
              Expressions.call(m,
                  Iterables.concat(
                      Collections.singletonList(add.accumulator().get(0)),
                      add.arguments(),
                      Collections.singletonList(
                          Expressions.constant(function.getNullClause()))))));
    }

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      return Expressions.call(BuiltInMethod.JSONIZE.method,
          result.accumulator().get(0));
    }
  }

  /** Implementor for the {@code TRIM} function. */
  private static class TrimImplementor implements NotNullImplementor {
    public Expression implement(RexToLixTranslator translator, RexCall call,
        List<Expression> translatedOperands) {
      final boolean strict = !translator.conformance.allowExtendedTrim();
      final Object value =
          ((ConstantExpression) translatedOperands.get(0)).value;
      SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag) value;
      return Expressions.call(
          BuiltInMethod.TRIM.method,
          Expressions.constant(
              flag == SqlTrimFunction.Flag.BOTH
              || flag == SqlTrimFunction.Flag.LEADING),
          Expressions.constant(
              flag == SqlTrimFunction.Flag.BOTH
              || flag == SqlTrimFunction.Flag.TRAILING),
          translatedOperands.get(1),
          translatedOperands.get(2),
          Expressions.constant(strict));
    }
  }

  /** Implementor for the {@code MONTHNAME} and {@code DAYNAME} functions.
   * Each takes a {@link java.util.Locale} argument. */
  private static class PeriodNameImplementor extends MethodNameImplementor {
    private final BuiltInMethod timestampMethod;
    private final BuiltInMethod dateMethod;

    PeriodNameImplementor(String methodName, BuiltInMethod timestampMethod,
        BuiltInMethod dateMethod) {
      super(methodName);
      this.timestampMethod = timestampMethod;
      this.dateMethod = dateMethod;
    }

    @Override public Expression implement(RexToLixTranslator translator,
        RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      final RelDataType type = call.operands.get(0).getType();
      switch (type.getSqlTypeName()) {
      case TIMESTAMP:
        return getExpression(translator, operand, timestampMethod);
      case DATE:
        return getExpression(translator, operand, dateMethod);
      default:
        throw new AssertionError("unknown type " + type);
      }
    }

    protected Expression getExpression(RexToLixTranslator translator,
        Expression operand, BuiltInMethod builtInMethod) {
      final MethodCallExpression locale =
          Expressions.call(BuiltInMethod.LOCALE.method, translator.getRoot());
      return Expressions.call(builtInMethod.method.getDeclaringClass(),
          builtInMethod.method.getName(), operand, locale);
    }
  }

  /** Implementor for the {@code FLOOR} and {@code CEIL} functions. */
  private static class FloorImplementor extends MethodNameImplementor {
    final Method timestampMethod;
    final Method dateMethod;

    FloorImplementor(String methodName, Method timestampMethod,
        Method dateMethod) {
      super(methodName);
      this.timestampMethod = timestampMethod;
      this.dateMethod = dateMethod;
    }

    public Expression implement(RexToLixTranslator translator, RexCall call,
        List<Expression> translatedOperands) {
      switch (call.getOperands().size()) {
      case 1:
        switch (call.getType().getSqlTypeName()) {
        case BIGINT:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          return translatedOperands.get(0);
        default:
          return super.implement(translator, call, translatedOperands);
        }

      case 2:
        final Type type;
        final Method floorMethod;
        Expression operand = translatedOperands.get(0);
        switch (call.getType().getSqlTypeName()) {
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          operand = Expressions.call(
              BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
          // fall through
        case TIMESTAMP:
          type = long.class;
          floorMethod = timestampMethod;
          break;
        default:
          type = int.class;
          floorMethod = dateMethod;
        }
        final ConstantExpression tur =
            (ConstantExpression) translatedOperands.get(1);
        final TimeUnitRange timeUnitRange = (TimeUnitRange) tur.value;
        switch (timeUnitRange) {
        case YEAR:
        case MONTH:
          return Expressions.call(floorMethod, tur,
              call(operand, type, TimeUnit.DAY));
        case NANOSECOND:
        default:
          return call(operand, type, timeUnitRange.startUnit);
        }

      default:
        throw new AssertionError();
      }
    }

    private Expression call(Expression operand, Type type,
        TimeUnit timeUnit) {
      return Expressions.call(SqlFunctions.class, methodName,
          Types.castIfNecessary(type, operand),
          Types.castIfNecessary(type,
              Expressions.constant(timeUnit.multiplier)));
    }
  }

  /** Implementor for a function that generates calls to a given method. */
  private static class MethodImplementor implements NotNullImplementor {
    protected final Method method;

    MethodImplementor(Method method) {
      this.method = method;
    }

    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      final Expression expression;
      if (Modifier.isStatic(method.getModifiers())) {
        expression = Expressions.call(method, translatedOperands);
      } else {
        expression = Expressions.call(translatedOperands.get(0), method,
            Util.skip(translatedOperands, 1));
      }

      final Type returnType =
          translator.typeFactory.getJavaClass(call.getType());
      return Types.castIfNecessary(returnType, expression);
    }
  }

  /** Implementor for SQL functions that generates calls to a given method name.
   *
   * <p>Use this, as opposed to {@link MethodImplementor}, if the SQL function
   * is overloaded; then you can use one implementor for several overloads. */
  private static class MethodNameImplementor implements NotNullImplementor {
    protected final String methodName;

    MethodNameImplementor(String methodName) {
      this.methodName = methodName;
    }

    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      return Expressions.call(
          SqlFunctions.class,
          methodName,
          translatedOperands);
    }
  }

  /** Implementor for binary operators. */
  private static class BinaryImplementor implements NotNullImplementor {
    /** Types that can be arguments to comparison operators such as
     * {@code <}. */
    private static final List<Primitive> COMP_OP_TYPES =
        ImmutableList.of(
            Primitive.BYTE,
            Primitive.CHAR,
            Primitive.SHORT,
            Primitive.INT,
            Primitive.LONG,
            Primitive.FLOAT,
            Primitive.DOUBLE);

    private static final List<SqlBinaryOperator> COMPARISON_OPERATORS =
        ImmutableList.of(
            SqlStdOperatorTable.LESS_THAN,
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            SqlStdOperatorTable.GREATER_THAN,
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    public static final String METHOD_POSTFIX_FOR_ANY_TYPE = "Any";

    private final ExpressionType expressionType;
    private final String backupMethodName;

    BinaryImplementor(
        ExpressionType expressionType,
        String backupMethodName) {
      this.expressionType = expressionType;
      this.backupMethodName = backupMethodName;
    }

    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> expressions) {
      // neither nullable:
      //   return x OP y
      // x nullable
      //   null_returns_null
      //     return x == null ? null : x OP y
      //   ignore_null
      //     return x == null ? null : y
      // x, y both nullable
      //   null_returns_null
      //     return x == null || y == null ? null : x OP y
      //   ignore_null
      //     return x == null ? y : y == null ? x : x OP y
      if (backupMethodName != null) {
        // If one or both operands have ANY type, use the late-binding backup
        // method.
        if (anyAnyOperands(call)) {
          return callBackupMethodAnyType(translator, call, expressions);
        }

        final Type type0 = expressions.get(0).getType();
        final Type type1 = expressions.get(1).getType();
        final SqlBinaryOperator op = (SqlBinaryOperator) call.getOperator();
        final Primitive primitive = Primitive.ofBoxOr(type0);
        if (primitive == null
            || type1 == BigDecimal.class
            || COMPARISON_OPERATORS.contains(op)
            && !COMP_OP_TYPES.contains(primitive)) {
          return Expressions.call(SqlFunctions.class, backupMethodName,
              expressions);
        }
      }

      final Type returnType =
          translator.typeFactory.getJavaClass(call.getType());
      return Types.castIfNecessary(returnType,
          Expressions.makeBinary(expressionType, expressions.get(0),
              expressions.get(1)));
    }

    /** Returns whether any of a call's operands have ANY type. */
    private boolean anyAnyOperands(RexCall call) {
      for (RexNode operand : call.operands) {
        if (operand.getType().getSqlTypeName() == SqlTypeName.ANY) {
          return true;
        }
      }
      return false;
    }

    private Expression callBackupMethodAnyType(RexToLixTranslator translator,
        RexCall call, List<Expression> expressions) {
      final String backupMethodNameForAnyType =
          backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE;

      // one or both of parameter(s) is(are) ANY type
      final Expression expression0 = maybeBox(expressions.get(0));
      final Expression expression1 = maybeBox(expressions.get(1));
      return Expressions.call(SqlFunctions.class, backupMethodNameForAnyType,
          expression0, expression1);
    }

    private Expression maybeBox(Expression expression) {
      final Primitive primitive = Primitive.of(expression.getType());
      if (primitive != null) {
        expression = Expressions.box(expression, primitive);
      }
      return expression;
    }
  }

  /** Implementor for unary operators. */
  private static class UnaryImplementor implements NotNullImplementor {
    private final ExpressionType expressionType;

    UnaryImplementor(ExpressionType expressionType) {
      this.expressionType = expressionType;
    }

    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      final Expression operand = translatedOperands.get(0);
      final UnaryExpression e = Expressions.makeUnary(expressionType, operand);
      if (e.type.equals(operand.type)) {
        return e;
      }
      // Certain unary operators do not preserve type. For example, the "-"
      // operator applied to a "byte" expression returns an "int".
      return Expressions.convert_(e, operand.type);
    }
  }

  /** Implementor for the {@code EXTRACT(unit FROM datetime)} function. */
  private static class ExtractImplementor implements NotNullImplementor {
    public Expression implement(RexToLixTranslator translator, RexCall call,
        List<Expression> translatedOperands) {
      final TimeUnitRange timeUnitRange =
          (TimeUnitRange) ((ConstantExpression) translatedOperands.get(0)).value;
      final TimeUnit unit = timeUnitRange.startUnit;
      Expression operand = translatedOperands.get(1);
      final SqlTypeName sqlTypeName =
          call.operands.get(1).getType().getSqlTypeName();
      switch (unit) {
      case MILLENNIUM:
      case CENTURY:
      case YEAR:
      case QUARTER:
      case MONTH:
      case DAY:
      case DOW:
      case DECADE:
      case DOY:
      case ISODOW:
      case ISOYEAR:
      case WEEK:
        switch (sqlTypeName) {
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
          break;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          operand = Expressions.call(
              BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
          // fall through
        case TIMESTAMP:
          operand = Expressions.call(BuiltInMethod.FLOOR_DIV.method,
              operand, Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
          // fall through
        case DATE:
          return Expressions.call(BuiltInMethod.UNIX_DATE_EXTRACT.method,
              translatedOperands.get(0), operand);
        default:
          throw new AssertionError("unexpected " + sqlTypeName);
        }
        break;
      case MILLISECOND:
      case MICROSECOND:
      case NANOSECOND:
        if (sqlTypeName == SqlTypeName.DATE) {
          return Expressions.constant(0L);
        }
        operand = mod(operand, TimeUnit.MINUTE.multiplier.longValue());
        return Expressions.multiply(
            operand, Expressions.constant((long) (1 / unit.multiplier.doubleValue())));
      case EPOCH:
        switch (sqlTypeName) {
        case DATE:
          // convert to milliseconds
          operand = Expressions.multiply(operand,
              Expressions.constant(TimeUnit.DAY.multiplier.longValue()));
          // fall through
        case TIMESTAMP:
          // convert to seconds
          return Expressions.divide(operand,
              Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          operand = Expressions.call(
              BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot()));
          return Expressions.divide(operand,
              Expressions.constant(TimeUnit.SECOND.multiplier.longValue()));
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
          // no convertlet conversion, pass it as extract
          throw new AssertionError("unexpected " + sqlTypeName);
        }
        break;
      case HOUR:
      case MINUTE:
      case SECOND:
        switch (sqlTypeName) {
        case DATE:
          return Expressions.multiply(operand, Expressions.constant(0L));
        }
        break;
      }

      operand = mod(operand, getFactor(unit));
      if (unit == TimeUnit.QUARTER) {
        operand = Expressions.subtract(operand, Expressions.constant(1L));
      }
      operand = Expressions.divide(operand,
          Expressions.constant(unit.multiplier.longValue()));
      if (unit == TimeUnit.QUARTER) {
        operand = Expressions.add(operand, Expressions.constant(1L));
      }
      return operand;
    }

  }

  private static Expression mod(Expression operand, long factor) {
    if (factor == 1L) {
      return operand;
    } else {
      return Expressions.call(BuiltInMethod.FLOOR_MOD.method,
          operand, Expressions.constant(factor));
    }
  }

  private static long getFactor(TimeUnit unit) {
    switch (unit) {
    case DAY:
      return 1L;
    case HOUR:
      return TimeUnit.DAY.multiplier.longValue();
    case MINUTE:
      return TimeUnit.HOUR.multiplier.longValue();
    case SECOND:
      return TimeUnit.MINUTE.multiplier.longValue();
    case MILLISECOND:
      return TimeUnit.SECOND.multiplier.longValue();
    case MONTH:
      return TimeUnit.YEAR.multiplier.longValue();
    case QUARTER:
      return TimeUnit.YEAR.multiplier.longValue();
    case YEAR:
    case DECADE:
    case CENTURY:
    case MILLENNIUM:
      return 1L;
    default:
      throw Util.unexpected(unit);
    }
  }

  /** Implementor for the SQL {@code CASE} operator. */
  private static class CaseImplementor implements CallImplementor {
    public Expression implement(RexToLixTranslator translator, RexCall call,
        NullAs nullAs) {
      return implementRecurse(translator, call, nullAs, 0);
    }

    private Expression implementRecurse(RexToLixTranslator translator,
        RexCall call, NullAs nullAs, int i) {
      List<RexNode> operands = call.getOperands();
      if (i == operands.size() - 1) {
        // the "else" clause
        return translator.translate(
            translator.builder.ensureType(
                call.getType(), operands.get(i), false), nullAs);
      } else {
        Expression ifTrue;
        try {
          ifTrue = translator.translate(
              translator.builder.ensureType(call.getType(),
                  operands.get(i + 1),
                  false), nullAs);
        } catch (RexToLixTranslator.AlwaysNull e) {
          ifTrue = null;
        }

        Expression ifFalse;
        try {
          ifFalse = implementRecurse(translator, call, nullAs, i + 2);
        } catch (RexToLixTranslator.AlwaysNull e) {
          if (ifTrue == null) {
            throw RexToLixTranslator.AlwaysNull.INSTANCE;
          }
          ifFalse = null;
        }

        Expression test = translator.translate(operands.get(i), NullAs.FALSE);

        return ifTrue == null || ifFalse == null
            ? Util.first(ifTrue, ifFalse)
            : Expressions.condition(test, ifTrue, ifFalse);
      }
    }
  }

  /** Implementor for the SQL {@code COALESCE} operator. */
  private static class CoalesceImplementor implements CallImplementor {
    public Expression implement(RexToLixTranslator translator, RexCall call,
        NullAs nullAs) {
      return implementRecurse(translator, call.operands, nullAs);
    }

    private Expression implementRecurse(RexToLixTranslator translator,
        List<RexNode> operands, NullAs nullAs) {
      if (operands.size() == 1) {
        return translator.translate(operands.get(0));
      } else {
        return Expressions.condition(
            translator.translate(operands.get(0), NullAs.IS_NULL),
            translator.translate(operands.get(0), nullAs),
            implementRecurse(translator, Util.skip(operands), nullAs));
      }
    }
  }

  /** Implementor for the SQL {@code CAST} function that optimizes if, say, the
   * argument is already of the desired type. */
  private static class CastOptimizedImplementor implements CallImplementor {
    private final CallImplementor accurate;

    private CastOptimizedImplementor() {
      accurate = createImplementor(new CastImplementor(),
          NullPolicy.STRICT, false);
    }

    public Expression implement(RexToLixTranslator translator, RexCall call,
        NullAs nullAs) {
      // Short-circuit if no cast is required
      RexNode arg = call.getOperands().get(0);
      if (call.getType().equals(arg.getType())) {
        // No cast required, omit cast
        return translator.translate(arg, nullAs);
      }
      if (SqlTypeUtil.equalSansNullability(translator.typeFactory,
          call.getType(), arg.getType())
          && nullAs == NullAs.NULL
          && translator.deref(arg) instanceof RexLiteral) {
        return RexToLixTranslator.translateLiteral(
            (RexLiteral) translator.deref(arg), call.getType(),
            translator.typeFactory, nullAs);
      }
      return accurate.implement(translator, call, nullAs);
    }
  }

  /** Implementor for the SQL {@code CAST} operator. */
  private static class CastImplementor implements NotNullImplementor {
    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      assert call.getOperands().size() == 1;
      final RelDataType sourceType = call.getOperands().get(0).getType();
      // It's only possible for the result to be null if both expression
      // and target type are nullable. We assume that the caller did not
      // make a mistake. If expression looks nullable, caller WILL have
      // checked that expression is not null before calling us.
      final boolean nullable =
          translator.isNullable(call)
              && sourceType.isNullable()
              && !Primitive.is(translatedOperands.get(0).getType());
      final RelDataType targetType =
          translator.nullifyType(call.getType(), nullable);
      return translator.translateCast(sourceType,
          targetType,
          translatedOperands.get(0));
    }
  }

  /** Implementor for the {@code REINTERPRET} internal SQL operator. */
  private static class ReinterpretImplementor implements NotNullImplementor {
    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      assert call.getOperands().size() == 1;
      return translatedOperands.get(0);
    }
  }

  /** Implementor for a value-constructor. */
  private static class ValueConstructorImplementor
      implements CallImplementor {
    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        NullAs nullAs) {
      return translator.translateConstructor(call.getOperands(),
          call.getOperator().getKind());
    }
  }

  /** Implementor for the {@code ITEM} SQL operator. */
  private static class ItemImplementor
      implements CallImplementor {
    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        NullAs nullAs) {
      final MethodImplementor implementor =
          getImplementor(
              call.getOperands().get(0).getType().getSqlTypeName());
      // Since we follow PostgreSQL's semantics that an out-of-bound reference
      // returns NULL, x[y] can return null even if x and y are both NOT NULL.
      // (In SQL standard semantics, an out-of-bound reference to an array
      // throws an exception.)
      final NullPolicy nullPolicy = NullPolicy.ANY;
      return implementNullSemantics0(translator, call, nullAs, nullPolicy,
          false, implementor);
    }

    private MethodImplementor getImplementor(SqlTypeName sqlTypeName) {
      switch (sqlTypeName) {
      case ARRAY:
        return new MethodImplementor(BuiltInMethod.ARRAY_ITEM.method);
      case MAP:
        return new MethodImplementor(BuiltInMethod.MAP_ITEM.method);
      default:
        return new MethodImplementor(BuiltInMethod.ANY_ITEM.method);
      }
    }
  }

  /** Implementor for SQL system functions.
   *
   * <p>Several of these are represented internally as constant values, set
   * per execution. */
  private static class SystemFunctionImplementor
      implements CallImplementor {
    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        NullAs nullAs) {
      switch (nullAs) {
      case IS_NULL:
        return Expressions.constant(false);
      case IS_NOT_NULL:
        return Expressions.constant(true);
      }
      final SqlOperator op = call.getOperator();
      final Expression root = translator.getRoot();
      if (op == CURRENT_USER
          || op == SESSION_USER
          || op == USER) {
        return Expressions.call(BuiltInMethod.USER.method, root);
      } else if (op == SYSTEM_USER) {
        return Expressions.call(BuiltInMethod.SYSTEM_USER.method, root);
      } else if (op == CURRENT_PATH
          || op == CURRENT_ROLE
          || op == CURRENT_CATALOG) {
        // By default, the CURRENT_ROLE and CURRENT_CATALOG functions return the
        // empty string because a role or a catalog has to be set explicitly.
        return Expressions.constant("");
      } else if (op == CURRENT_TIMESTAMP) {
        return Expressions.call(BuiltInMethod.CURRENT_TIMESTAMP.method, root);
      } else if (op == CURRENT_TIME) {
        return Expressions.call(BuiltInMethod.CURRENT_TIME.method, root);
      } else if (op == CURRENT_DATE) {
        return Expressions.call(BuiltInMethod.CURRENT_DATE.method, root);
      } else if (op == LOCALTIMESTAMP) {
        return Expressions.call(BuiltInMethod.LOCAL_TIMESTAMP.method, root);
      } else if (op == LOCALTIME) {
        return Expressions.call(BuiltInMethod.LOCAL_TIME.method, root);
      } else {
        throw new AssertionError("unknown function " + op);
      }
    }
  }

  /** Implements "IS XXX" operations such as "IS NULL"
   * or "IS NOT TRUE".
   *
   * <p>What these operators have in common:</p>
   * 1. They return TRUE or FALSE, never NULL.
   * 2. Of the 3 input values (TRUE, FALSE, NULL) they return TRUE for 1 or 2,
   *    FALSE for the other 2 or 1.
   */
  private static class IsXxxImplementor
      implements CallImplementor {
    private final Boolean seek;
    private final boolean negate;

    IsXxxImplementor(Boolean seek, boolean negate) {
      this.seek = seek;
      this.negate = negate;
    }

    public Expression implement(
        RexToLixTranslator translator, RexCall call, NullAs nullAs) {
      List<RexNode> operands = call.getOperands();
      assert operands.size() == 1;
      switch (nullAs) {
      case IS_NOT_NULL:
        return BOXED_TRUE_EXPR;
      case IS_NULL:
        return BOXED_FALSE_EXPR;
      }
      if (seek == null) {
        return translator.translate(operands.get(0),
            negate ? NullAs.IS_NOT_NULL : NullAs.IS_NULL);
      } else {
        return maybeNegate(negate == seek,
            translator.translate(operands.get(0),
                seek ? NullAs.FALSE : NullAs.TRUE));
      }
    }
  }

  /** Implementor for the {@code NOT} operator. */
  private static class NotImplementor implements NotNullImplementor {
    private final NotNullImplementor implementor;

    NotImplementor(NotNullImplementor implementor) {
      this.implementor = implementor;
    }

    private static NotNullImplementor of(NotNullImplementor implementor) {
      return new NotImplementor(implementor);
    }

    public Expression implement(
        RexToLixTranslator translator,
        RexCall call,
        List<Expression> translatedOperands) {
      final Expression expression =
          implementor.implement(translator, call, translatedOperands);
      return Expressions.not(expression);
    }
  }

  /** Implementor for various datetime arithmetic. */
  private static class DatetimeArithmeticImplementor
      implements NotNullImplementor {
    public Expression implement(RexToLixTranslator translator, RexCall call,
        List<Expression> translatedOperands) {
      final RexNode operand0 = call.getOperands().get(0);
      Expression trop0 = translatedOperands.get(0);
      final SqlTypeName typeName1 =
          call.getOperands().get(1).getType().getSqlTypeName();
      Expression trop1 = translatedOperands.get(1);
      final SqlTypeName typeName = call.getType().getSqlTypeName();
      switch (operand0.getType().getSqlTypeName()) {
      case DATE:
        switch (typeName) {
        case TIMESTAMP:
          trop0 = Expressions.convert_(
              Expressions.multiply(trop0,
                  Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
              long.class);
          break;
        default:
          switch (typeName1) {
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
            trop1 = Expressions.convert_(
                Expressions.divide(trop1,
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                int.class);
          }
        }
        break;
      case TIME:
        trop1 = Expressions.convert_(trop1, int.class);
        break;
      }
      switch (typeName1) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        switch (call.getKind()) {
        case MINUS:
          trop1 = Expressions.negate(trop1);
        }
        switch (typeName) {
        case TIME:
          return Expressions.convert_(trop0, long.class);
        default:
          final BuiltInMethod method =
              operand0.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP
                  ? BuiltInMethod.ADD_MONTHS
                  : BuiltInMethod.ADD_MONTHS_INT;
          return Expressions.call(method.method, trop0, trop1);
        }

      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        switch (call.getKind()) {
        case MINUS:
          return normalize(typeName, Expressions.subtract(trop0, trop1));
        default:
          return normalize(typeName, Expressions.add(trop0, trop1));
        }

      default:
        switch (call.getKind()) {
        case MINUS:
          switch (typeName) {
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            return Expressions.call(BuiltInMethod.SUBTRACT_MONTHS.method,
                trop0, trop1);
          }
          TimeUnit fromUnit =
              typeName1 == SqlTypeName.DATE ? TimeUnit.DAY : TimeUnit.MILLISECOND;
          TimeUnit toUnit = TimeUnit.MILLISECOND;
          return multiplyDivide(
              Expressions.convert_(Expressions.subtract(trop0, trop1),
                  (Class) long.class),
              fromUnit.multiplier, toUnit.multiplier);
        default:
          throw new AssertionError(call);
        }
      }
    }

    /** Normalizes a TIME value into 00:00:00..23:59:39. */
    private Expression normalize(SqlTypeName typeName, Expression e) {
      switch (typeName) {
      case TIME:
        return Expressions.call(BuiltInMethod.FLOOR_MOD.method, e,
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
      default:
        return e;
      }
    }
  }
}

// End RexImpTable.java
