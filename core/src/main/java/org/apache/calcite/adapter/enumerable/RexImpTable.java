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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.OptimizeShuttle;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.schema.ImplementableAggFunction;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeConstructorFunction;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlJsonArrayAggAggFunction;
import org.apache.calcite.sql.fun.SqlJsonObjectAggAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.adapter.enumerable.EnumUtils.generateCollatorExpression;
import static org.apache.calcite.linq4j.tree.ExpressionType.Add;
import static org.apache.calcite.linq4j.tree.ExpressionType.Divide;
import static org.apache.calcite.linq4j.tree.ExpressionType.Equal;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Multiply;
import static org.apache.calcite.linq4j.tree.ExpressionType.Negate;
import static org.apache.calcite.linq4j.tree.ExpressionType.NotEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Subtract;
import static org.apache.calcite.linq4j.tree.ExpressionType.UnaryPlus;
import static org.apache.calcite.sql.fun.SqlInternalOperators.THROW_UNLESS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_CONCAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_CONCAT_AGG;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.BOOL_AND;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.BOOL_OR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHAR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.COMPRESS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT2;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT_FUNCTION;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.COSH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DATE_FROM_UNIX_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DAYNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.DIFFERENCE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXISTS_NODE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXTRACT_VALUE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.EXTRACT_XML;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_DEPTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_KEYS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_PRETTY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_REMOVE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_STORAGE_SIZE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.JSON_TYPE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LEFT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LOGICAL_AND;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LOGICAL_OR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MD5;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MONTHNAME;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REPEAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RIGHT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RLIKE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SHA1;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SINH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SOUNDEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.STRCMP;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TANH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_SECONDS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIMESTAMP_TRUNC;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TIME_TRUNC;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_DATE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MICROS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_MILLIS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.UNIX_SECONDS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.XML_TRANSFORM;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.BIT_XOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CARDINALITY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CBRT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CLASSIFIER;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EVERY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EXTRACT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FIRST_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.FUSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GROUPING_ID;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.INITCAP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.INTERSECTION;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_TYPE_OPERATOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.JSON_VALUE_EXPRESSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAG;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MODE;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTH_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NTILE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OCTET_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OVERLAY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER;
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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIMILAR_TO;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SINGLE_VALUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SLICE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SOME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.STRUCT_ACCESS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBMULTISET_OF;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM0;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SYSTEM_USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRIM;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TRUNCATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TUMBLE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UNARY_PLUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.USER;

import static java.util.Objects.requireNonNull;

/**
 * Contains implementations of Rex operators as Java code.
 */
public class RexImpTable {
  public static final RexImpTable INSTANCE = new RexImpTable();

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

  private final Map<SqlOperator, RexCallImplementor> map = new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends AggImplementor>> aggMap =
      new HashMap<>();
  private final Map<SqlAggFunction, Supplier<? extends WinAggImplementor>> winAggMap =
      new HashMap<>();
  private final Map<SqlMatchFunction, Supplier<? extends MatchImplementor>> matchMap =
      new HashMap<>();
  private final Map<SqlOperator, Supplier<? extends TableFunctionCallImplementor>>
      tvfImplementorMap = new HashMap<>();

  @SuppressWarnings("method.invocation.invalid")
  RexImpTable() {
    defineMethod(THROW_UNLESS, BuiltInMethod.THROW_UNLESS.method, NullPolicy.NONE);
    defineMethod(ROW, BuiltInMethod.ARRAY.method, NullPolicy.ALL);
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
    defineMethod(CHR, BuiltInMethod.CHAR_FROM_UTF8.method, NullPolicy.STRICT);
    defineMethod(CHARACTER_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
        NullPolicy.STRICT);
    defineMethod(CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
        NullPolicy.STRICT);
    defineMethod(OCTET_LENGTH, BuiltInMethod.OCTET_LENGTH.method,
        NullPolicy.STRICT);
    map.put(CONCAT, new ConcatImplementor());
    defineMethod(CONCAT_FUNCTION, BuiltInMethod.MULTI_STRING_CONCAT.method,
        NullPolicy.STRICT);
    defineMethod(CONCAT2, BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT);
    defineMethod(OVERLAY, BuiltInMethod.OVERLAY.method, NullPolicy.STRICT);
    defineMethod(POSITION, BuiltInMethod.POSITION.method, NullPolicy.STRICT);
    defineMethod(ASCII, BuiltInMethod.ASCII.method, NullPolicy.STRICT);
    defineMethod(CHAR, BuiltInMethod.CHAR_FROM_ASCII.method,
        NullPolicy.SEMI_STRICT);
    defineMethod(REPEAT, BuiltInMethod.REPEAT.method, NullPolicy.STRICT);
    defineMethod(SPACE, BuiltInMethod.SPACE.method, NullPolicy.STRICT);
    defineMethod(STRCMP, BuiltInMethod.STRCMP.method, NullPolicy.STRICT);
    defineMethod(SOUNDEX, BuiltInMethod.SOUNDEX.method, NullPolicy.STRICT);
    defineMethod(DIFFERENCE, BuiltInMethod.DIFFERENCE.method, NullPolicy.STRICT);
    defineMethod(REVERSE, BuiltInMethod.REVERSE.method, NullPolicy.STRICT);

    map.put(TRIM, new TrimImplementor());

    // logical
    map.put(AND, new LogicalAndImplementor());
    map.put(OR, new LogicalOrImplementor());
    map.put(NOT, new LogicalNotImplementor());

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
    defineUnary(UNARY_MINUS, Negate, NullPolicy.STRICT,
        BuiltInMethod.BIG_DECIMAL_NEGATE.getMethodName());
    defineUnary(UNARY_PLUS, UnaryPlus, NullPolicy.STRICT, null);

    defineMethod(MOD, "mod", NullPolicy.STRICT);
    defineMethod(EXP, "exp", NullPolicy.STRICT);
    defineMethod(POWER, "power", NullPolicy.STRICT);
    defineMethod(LN, "ln", NullPolicy.STRICT);
    defineMethod(LOG10, "log10", NullPolicy.STRICT);
    defineMethod(ABS, "abs", NullPolicy.STRICT);

    map.put(RAND, new RandImplementor());
    map.put(RAND_INTEGER, new RandIntegerImplementor());

    defineMethod(ACOS, "acos", NullPolicy.STRICT);
    defineMethod(ASIN, "asin", NullPolicy.STRICT);
    defineMethod(ATAN, "atan", NullPolicy.STRICT);
    defineMethod(ATAN2, "atan2", NullPolicy.STRICT);
    defineMethod(CBRT, "cbrt", NullPolicy.STRICT);
    defineMethod(COS, "cos", NullPolicy.STRICT);
    defineMethod(COSH, "cosh", NullPolicy.STRICT);
    defineMethod(COT, "cot", NullPolicy.STRICT);
    defineMethod(DEGREES, "degrees", NullPolicy.STRICT);
    defineMethod(RADIANS, "radians", NullPolicy.STRICT);
    defineMethod(ROUND, "sround", NullPolicy.STRICT);
    defineMethod(SIGN, "sign", NullPolicy.STRICT);
    defineMethod(SIN, "sin", NullPolicy.STRICT);
    defineMethod(SINH, "sinh", NullPolicy.STRICT);
    defineMethod(TAN, "tan", NullPolicy.STRICT);
    defineMethod(TANH, "tanh", NullPolicy.STRICT);
    defineMethod(TRUNCATE, "struncate", NullPolicy.STRICT);

    map.put(PI, new PiImplementor());

    // datetime
    map.put(DATETIME_PLUS, new DatetimeArithmeticImplementor());
    map.put(MINUS_DATE, new DatetimeArithmeticImplementor());
    map.put(EXTRACT, new ExtractImplementor());
    map.put(FLOOR,
        new FloorImplementor(BuiltInMethod.FLOOR.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
            BuiltInMethod.UNIX_DATE_FLOOR.method));
    map.put(CEIL,
        new FloorImplementor(BuiltInMethod.CEIL.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
            BuiltInMethod.UNIX_DATE_CEIL.method));

    // TIMESTAMP_TRUNC and TIME_TRUNC methods are syntactic sugar for standard datetime FLOOR
    map.put(TIMESTAMP_TRUNC,
        new FloorImplementor(BuiltInMethod.FLOOR.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
            BuiltInMethod.UNIX_DATE_FLOOR.method));
    map.put(TIME_TRUNC,
        new FloorImplementor(BuiltInMethod.FLOOR.method.getName(),
            BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
            BuiltInMethod.UNIX_DATE_FLOOR.method));


    defineMethod(LAST_DAY, "lastDay", NullPolicy.STRICT);
    map.put(DAYNAME,
        new PeriodNameImplementor("dayName",
            BuiltInMethod.DAYNAME_WITH_TIMESTAMP,
            BuiltInMethod.DAYNAME_WITH_DATE));
    map.put(MONTHNAME,
        new PeriodNameImplementor("monthName",
            BuiltInMethod.MONTHNAME_WITH_TIMESTAMP,
            BuiltInMethod.MONTHNAME_WITH_DATE));
    defineMethod(TIMESTAMP_SECONDS, "timestampSeconds", NullPolicy.STRICT);
    defineMethod(TIMESTAMP_MILLIS, "timestampMillis", NullPolicy.STRICT);
    defineMethod(TIMESTAMP_MICROS, "timestampMicros", NullPolicy.STRICT);
    defineMethod(UNIX_SECONDS, "unixSeconds", NullPolicy.STRICT);
    defineMethod(UNIX_MILLIS, "unixMillis", NullPolicy.STRICT);
    defineMethod(UNIX_MICROS, "unixMicros", NullPolicy.STRICT);
    defineMethod(DATE_FROM_UNIX_DATE, "dateFromUnixDate", NullPolicy.STRICT);
    defineMethod(UNIX_DATE, "unixDate", NullPolicy.STRICT);

    map.put(IS_NULL, new IsNullImplementor());
    map.put(IS_NOT_NULL, new IsNotNullImplementor());
    map.put(IS_TRUE, new IsTrueImplementor());
    map.put(IS_NOT_TRUE, new IsNotTrueImplementor());
    map.put(IS_FALSE, new IsFalseImplementor());
    map.put(IS_NOT_FALSE, new IsNotFalseImplementor());

    // LIKE, ILIKE and SIMILAR
    map.put(LIKE,
        new MethodImplementor(BuiltInMethod.LIKE.method, NullPolicy.STRICT,
            false));
    map.put(ILIKE,
        new MethodImplementor(BuiltInMethod.ILIKE.method, NullPolicy.STRICT,
            false));
    map.put(RLIKE,
        new MethodImplementor(BuiltInMethod.RLIKE.method, NullPolicy.STRICT,
            false));
    map.put(SIMILAR_TO,
        new MethodImplementor(BuiltInMethod.SIMILAR.method, NullPolicy.STRICT,
        false));

    // POSIX REGEX
    final MethodImplementor posixRegexImplementorCaseSensitive =
        new PosixRegexMethodImplementor(true);
    final MethodImplementor posixRegexImplementorCaseInsensitive =
        new PosixRegexMethodImplementor(false);
    map.put(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE,
        posixRegexImplementorCaseInsensitive);
    map.put(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
        posixRegexImplementorCaseSensitive);
    map.put(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE,
        NotImplementor.of(posixRegexImplementorCaseInsensitive));
    map.put(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE,
        NotImplementor.of(posixRegexImplementorCaseSensitive));
    map.put(REGEXP_REPLACE, new RegexpReplaceImplementor());

    // Multisets & arrays
    defineMethod(CARDINALITY, BuiltInMethod.COLLECTION_SIZE.method,
        NullPolicy.STRICT);
    defineMethod(ARRAY_LENGTH, BuiltInMethod.COLLECTION_SIZE.method,
        NullPolicy.STRICT);
    defineMethod(SLICE, BuiltInMethod.SLICE.method, NullPolicy.NONE);
    defineMethod(ELEMENT, BuiltInMethod.ELEMENT.method, NullPolicy.STRICT);
    defineMethod(STRUCT_ACCESS, BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY);
    defineMethod(MEMBER_OF, BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE);
    defineMethod(ARRAY_REVERSE, BuiltInMethod.ARRAY_REVERSE.method, NullPolicy.STRICT);
    map.put(ARRAY_CONCAT, new ArrayConcatImplementor());
    final MethodImplementor isEmptyImplementor =
        new MethodImplementor(BuiltInMethod.IS_EMPTY.method, NullPolicy.NONE,
            false);
    map.put(IS_EMPTY, isEmptyImplementor);
    map.put(IS_NOT_EMPTY, NotImplementor.of(isEmptyImplementor));
    final MethodImplementor isASetImplementor =
        new MethodImplementor(BuiltInMethod.IS_A_SET.method, NullPolicy.NONE,
            false);
    map.put(IS_A_SET, isASetImplementor);
    map.put(IS_NOT_A_SET, NotImplementor.of(isASetImplementor));
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
        new MethodImplementor(BuiltInMethod.SUBMULTISET_OF.method, NullPolicy.NONE, false);
    map.put(SUBMULTISET_OF, subMultisetImplementor);
    map.put(NOT_SUBMULTISET_OF, NotImplementor.of(subMultisetImplementor));

    map.put(COALESCE, new CoalesceImplementor());
    map.put(CAST, new CastImplementor());
    map.put(DATE, new CastImplementor());

    map.put(REINTERPRET, new ReinterpretImplementor());

    final RexCallImplementor value = new ValueConstructorImplementor();
    map.put(MAP_VALUE_CONSTRUCTOR, value);
    map.put(ARRAY_VALUE_CONSTRUCTOR, value);
    map.put(ITEM, new ItemImplementor());

    map.put(DEFAULT, new DefaultImplementor());

    // Sequences
    defineMethod(CURRENT_VALUE, BuiltInMethod.SEQUENCE_CURRENT_VALUE.method,
        NullPolicy.STRICT);
    defineMethod(NEXT_VALUE, BuiltInMethod.SEQUENCE_NEXT_VALUE.method,
        NullPolicy.STRICT);

    // Compression Operators
    defineMethod(COMPRESS, BuiltInMethod.COMPRESS.method, NullPolicy.ARG0);

    // Xml Operators
    defineMethod(EXTRACT_VALUE, BuiltInMethod.EXTRACT_VALUE.method, NullPolicy.ARG0);
    defineMethod(XML_TRANSFORM, BuiltInMethod.XML_TRANSFORM.method, NullPolicy.ARG0);
    defineMethod(EXTRACT_XML, BuiltInMethod.EXTRACT_XML.method, NullPolicy.ARG0);
    defineMethod(EXISTS_NODE, BuiltInMethod.EXISTS_NODE.method, NullPolicy.ARG0);

    // Json Operators
    defineMethod(JSON_VALUE_EXPRESSION,
        BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT);
    defineMethod(JSON_TYPE_OPERATOR,
        BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT);
    defineMethod(JSON_EXISTS, BuiltInMethod.JSON_EXISTS.method, NullPolicy.ARG0);
    map.put(JSON_VALUE,
        new JsonValueImplementor(BuiltInMethod.JSON_VALUE.method));
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
    map.put(IS_JSON_VALUE,
        new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method,
            NullPolicy.NONE, false));
    map.put(IS_JSON_OBJECT,
        new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method,
            NullPolicy.NONE, false));
    map.put(IS_JSON_ARRAY,
        new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method,
            NullPolicy.NONE, false));
    map.put(IS_JSON_SCALAR,
        new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method,
            NullPolicy.NONE, false));
    map.put(IS_NOT_JSON_VALUE,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_VALUE.method,
                NullPolicy.NONE, false)));
    map.put(IS_NOT_JSON_OBJECT,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_OBJECT.method,
                NullPolicy.NONE, false)));
    map.put(IS_NOT_JSON_ARRAY,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_ARRAY.method,
                NullPolicy.NONE, false)));
    map.put(IS_NOT_JSON_SCALAR,
        NotImplementor.of(
            new MethodImplementor(BuiltInMethod.IS_JSON_SCALAR.method,
                NullPolicy.NONE, false)));

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
    aggMap.put(SOME, minMax);
    aggMap.put(EVERY, minMax);
    aggMap.put(BOOL_AND, minMax);
    aggMap.put(BOOL_OR, minMax);
    aggMap.put(LOGICAL_AND, minMax);
    aggMap.put(LOGICAL_OR, minMax);
    final Supplier<BitOpImplementor> bitop =
        constructorSupplier(BitOpImplementor.class);
    aggMap.put(BIT_AND, bitop);
    aggMap.put(BIT_OR, bitop);
    aggMap.put(BIT_XOR, bitop);
    aggMap.put(SINGLE_VALUE, constructorSupplier(SingleValueImplementor.class));
    aggMap.put(COLLECT, constructorSupplier(CollectImplementor.class));
    aggMap.put(ARRAY_AGG, constructorSupplier(CollectImplementor.class));
    aggMap.put(LISTAGG, constructorSupplier(ListaggImplementor.class));
    aggMap.put(FUSION, constructorSupplier(FusionImplementor.class));
    aggMap.put(MODE, constructorSupplier(ModeImplementor.class));
    aggMap.put(ARRAY_CONCAT_AGG, constructorSupplier(FusionImplementor.class));
    aggMap.put(INTERSECTION, constructorSupplier(IntersectionImplementor.class));
    final Supplier<GroupingImplementor> grouping =
        constructorSupplier(GroupingImplementor.class);
    aggMap.put(GROUPING, grouping);
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
    matchMap.put(CLASSIFIER, ClassifierImplementor::new);
    matchMap.put(LAST, LastImplementor::new);

    tvfImplementorMap.put(TUMBLE, TumbleImplementor::new);
    tvfImplementorMap.put(HOP, HopImplementor::new);
    tvfImplementorMap.put(SESSION, SessionImplementor::new);
  }

  private static <T> Supplier<T> constructorSupplier(Class<T> klass) {
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

  public static CallImplementor createImplementor(
      final NotNullImplementor implementor,
      final NullPolicy nullPolicy,
      final boolean harmonize) {
    return (translator, call, nullAs) -> {
      final RexCallImplementor rexCallImplementor =
          createRexCallImplementor(implementor, nullPolicy, harmonize);
      final List<RexToLixTranslator.Result> arguments =
          translator.getCallOperandResult(call);
      assert arguments != null;
      final RexToLixTranslator.Result result =
          rexCallImplementor.implement(translator, call, arguments);
      return nullAs.handle(result.valueVariable);
    };
  }

  private void defineMethod(SqlOperator operator, String functionName,
      NullPolicy nullPolicy) {
    map.put(operator,
        new MethodNameImplementor(functionName, nullPolicy, false));
  }

  private void defineMethod(SqlOperator operator, Method method,
      NullPolicy nullPolicy) {
    map.put(operator, new MethodImplementor(method, nullPolicy, false));
  }

  private void defineUnary(SqlOperator operator, ExpressionType expressionType,
      NullPolicy nullPolicy, @Nullable String backupMethodName) {
    map.put(operator, new UnaryImplementor(expressionType, nullPolicy, backupMethodName));
  }

  private void defineBinary(SqlOperator operator, ExpressionType expressionType,
      NullPolicy nullPolicy, String backupMethodName) {
    map.put(operator,
        new BinaryImplementor(nullPolicy, true, expressionType,
            backupMethodName));
  }

  private static RexCallImplementor createRexCallImplementor(
      final NotNullImplementor implementor,
      final NullPolicy nullPolicy,
      final boolean harmonize) {
    return new AbstractRexCallImplementor(nullPolicy, harmonize) {
      @Override String getVariableName() {
        return "not_null_udf";
      }

      @Override Expression implementSafe(RexToLixTranslator translator,
          RexCall call, List<Expression> argValueList) {
        return implementor.implement(translator, call, argValueList);
      }
    };
  }

  private static RexCallImplementor wrapAsRexCallImplementor(
      final CallImplementor implementor) {
    return new AbstractRexCallImplementor(NullPolicy.NONE, false) {
      @Override String getVariableName() {
        return "udf";
      }

      @Override Expression implementSafe(RexToLixTranslator translator,
          RexCall call, List<Expression> argValueList) {
        return implementor.implement(translator, call, RexImpTable.NullAs.NULL);
      }
    };
  }

  public @Nullable RexCallImplementor get(final SqlOperator operator) {
    if (operator instanceof SqlUserDefinedFunction) {
      org.apache.calcite.schema.Function udf =
          ((SqlUserDefinedFunction) operator).getFunction();
      if (!(udf instanceof ImplementableFunction)) {
        throw new IllegalStateException("User defined function " + operator
            + " must implement ImplementableFunction");
      }
      CallImplementor implementor =
          ((ImplementableFunction) udf).getImplementor();
      return wrapAsRexCallImplementor(implementor);
    } else if (operator instanceof SqlTypeConstructorFunction) {
      return map.get(SqlStdOperatorTable.ROW);
    }
    return map.get(operator);
  }

  public @Nullable AggImplementor get(final SqlAggFunction aggregation,
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

  public MatchImplementor get(final SqlMatchFunction function) {
    final Supplier<? extends MatchImplementor> supplier =
        matchMap.get(function);
    if (supplier != null) {
      return supplier.get();
    } else {
      throw new IllegalStateException("Supplier should not be null");
    }
  }

  public TableFunctionCallImplementor get(final SqlWindowTableFunction operator) {
    final Supplier<? extends TableFunctionCallImplementor> supplier =
        tvfImplementorMap.get(operator);
    if (supplier != null) {
      return supplier.get();
    } else {
      throw new IllegalStateException("Supplier should not be null");
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
              Expressions.equal(operand, NULL_EXPR),
              NULL_EXPR,
              expression));
    }
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
          return EnumUtils.convert(x,
              Primitive.unbox(x.getType()));
        default:
          break;
        }
        break;
      default:
        break;
      }
      switch (this) {
      case NULL:
      case NOT_POSSIBLE:
        return x;
      case FALSE:
        return Expressions.call(BuiltInMethod.IS_TRUE.method, x);
      case TRUE:
        return Expressions.call(BuiltInMethod.IS_NOT_FALSE.method, x);
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
    Primitive p = Primitive.of(type);
    if (p != null) {
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
            EnumUtils.convert(add.arguments().get(0), acc.type));
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
      final boolean isMin = info.aggregation().kind == SqlKind.MIN;
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
      final boolean isMin = info.aggregation().kind == SqlKind.MIN;
      final Method method = (isMin
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
    @Override public List<Type> getStateType(AggContext info) {
      return Arrays.asList(boolean.class, info.returnType());
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
      List<Expression> acc = reset.accumulator();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0), Expressions.constant(false))));
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(1),
                  getDefaultValue(acc.get(1).getType()))));
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
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

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      return EnumUtils.convert(result.accumulator().get(1),
          info.returnType());
    }
  }

  /** Implementor for the {@code COLLECT} and {@code ARRAY_AGG}
   * aggregate functions. */
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

  /** Implementor for the {@code INTERSECTION} aggregate function. */
  static class IntersectionImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info, AggResetContext reset) {
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), Expressions.constant(null))));
    }

    @Override public void implementNotNullAdd(AggContext info, AggAddContext add) {
      BlockBuilder accumulatorIsNull = new BlockBuilder();
      accumulatorIsNull.add(
          Expressions.statement(
              Expressions.assign(add.accumulator().get(0), Expressions.new_(ArrayList.class))));
      accumulatorIsNull.add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_ADDALL.method, add.arguments().get(0))));

      BlockBuilder accumulatorNotNull = new BlockBuilder();
      accumulatorNotNull.add(
          Expressions.statement(
              Expressions.call(add.accumulator().get(0),
                  BuiltInMethod.COLLECTION_RETAIN_ALL.method,
                  add.arguments().get(0))
          )
      );

      add.currentBlock().add(
          Expressions.ifThenElse(
              Expressions.equal(add.accumulator().get(0), Expressions.constant(null)),
              accumulatorIsNull.toBlock(),
              accumulatorNotNull.toBlock()));
    }
  }

  /** Implementor for the {@code MODE} aggregate function. */
  static class ModeImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      // acc[0] = null;
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0),
                  Expressions.constant(null))));
      // acc[1] = new HashMap<>();
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(1),
                  Expressions.new_(HashMap.class))));
      // acc[2] = Long.valueOf(0);
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(2),
                  Expressions.constant(0, Long.class))));
    }

    @Override protected void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression currentArg = add.arguments().get(0);

      Expression currentResult = add.accumulator().get(0);
      Expression accMap = add.accumulator().get(1);
      Expression currentMaxNumber = add.accumulator().get(2);
      // the default number of occurrences is 0
      Expression getOrDefaultExpression =
          Expressions.call(accMap, BuiltInMethod.MAP_GET_OR_DEFAULT.method, currentArg,
              Expressions.constant(0, Long.class));
      // declare and assign the occurrences number about current value
      ParameterExpression currentNumber = Expressions.parameter(
          Long.class, add.currentBlock().newName("currentNumber"));
      add.currentBlock().add(Expressions.declare(0, currentNumber, null));
      add.currentBlock().add(
          Expressions.statement(
              Expressions.assign(currentNumber,
                  Expressions.add(Expressions.convert_(getOrDefaultExpression, Long.class),
                      Expressions.constant(1, Long.class)))));
      // update the occurrences number about current value
      Expression methodCallExpression2 =
          Expressions.call(accMap, BuiltInMethod.MAP_PUT.method, currentArg, currentNumber);
      add.currentBlock().add(Expressions.statement(methodCallExpression2));
      // update the most frequent value
      BlockBuilder thenBlock = new BlockBuilder(true, add.currentBlock());
      thenBlock.add(
          Expressions.statement(
              Expressions.assign(
                  currentMaxNumber, Expressions.convert_(currentNumber, Long.class))));
      thenBlock.add(
          Expressions.statement(Expressions.assign(currentResult, currentArg)));
      // if the maximum number of occurrences less than current value's occurrences number
      // than update
      add.currentBlock().add(
          Expressions.ifThen(
              Expressions.lessThan(currentMaxNumber, currentNumber), thenBlock.toBlock()));
    }

    @Override protected Expression implementNotNullResult(AggContext info,
        AggResultContext result) {
      return result.accumulator().get(0);
    }

    @Override public List<Type> getNotNullState(AggContext info) {
      List<Type> types = new ArrayList<>();
      // the most frequent value
      types.add(Object.class);
      // hashmap's key: value, hashmap's value: number of occurrences
      types.add(HashMap.class);
      // maximum number of occurrences about frequent value
      types.add(Long.class);
      return types;
    }
  }

  /** Implementor for the {@code FUSION} and {@code ARRAY_CONCAT_AGG}
   * aggregate functions. */
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

  /** Implementor for the {@code BIT_AND}, {@code BIT_OR} and {@code BIT_XOR} aggregate function. */
  static class BitOpImplementor extends StrictAggImplementor {
    @Override protected void implementNotNullReset(AggContext info,
        AggResetContext reset) {
      Expression start;
      if (SqlTypeUtil.isBinary(info.returnRelType())) {
        start = Expressions.field(null, ByteString.class, "EMPTY");
      } else {
        Object initValue = info.aggregation() == BIT_AND ? -1L : 0;
        start = Expressions.constant(initValue, info.returnType());
      }

      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(reset.accumulator().get(0), start)));
    }

    @Override public void implementNotNullAdd(AggContext info,
        AggAddContext add) {
      Expression acc = add.accumulator().get(0);
      Expression arg = add.arguments().get(0);
      SqlAggFunction aggregation = info.aggregation();

      final BuiltInMethod builtInMethod;
      switch (aggregation.kind) {
      case BIT_AND:
        builtInMethod = BuiltInMethod.BIT_AND;
        break;
      case BIT_OR:
        builtInMethod = BuiltInMethod.BIT_OR;
        break;
      case BIT_XOR:
        builtInMethod = BuiltInMethod.BIT_XOR;
        break;
      default:
        throw new IllegalArgumentException("Unknown " + aggregation.getName()
            + ". Only support bit_and, bit_or and bit_xor for bit aggregation function");
      }
      final Method method = builtInMethod.method;
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
    @Override public List<Type> getStateType(AggContext info) {
      return ImmutableList.of();
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
    }

    @Override public Expression implementResult(AggContext info,
        AggResultContext result) {
      final List<Integer> keys;
      switch (info.aggregation().kind) {
      case GROUPING: // "GROUPING(e, ...)", also "GROUPING_ID(e, ...)"
        keys = result.call().getArgList();
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
                Expressions.assign(acc.get(1), makeNew(afi))));
      }
      reset.currentBlock().add(
          Expressions.statement(
              Expressions.assign(acc.get(0),
                  Expressions.call(afi.isStatic
                      ? null
                      : acc.get(1), afi.initMethod))));
    }

    private static NewExpression makeNew(AggregateFunctionImpl afi) {
      try {
        Constructor<?> constructor = afi.declaringClass.getConstructor();
        Objects.requireNonNull(constructor, "constructor");
        return Expressions.new_(afi.declaringClass);
      } catch (NoSuchMethodException e) {
        // ignore, and try next constructor
      }
      try {
        Constructor<?> constructor =
            afi.declaringClass.getConstructor(FunctionContext.class);
        Objects.requireNonNull(constructor, "constructor");
        return Expressions.new_(afi.declaringClass,
            Expressions.call(BuiltInMethod.FUNCTION_CONTEXTS_OF.method,
                DataContext.ROOT,
                // TODO: pass in the values of arguments that are literals
                Expressions.newArrayBounds(Object.class, 1,
                    Expressions.constant(afi.getParameters().size()))));
      } catch (NoSuchMethodException e) {
        // This should never happen: validator should have made sure that the
        // class had an appropriate constructor.
        throw new AssertionError("no valid constructor for " + afi);
      }
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
          afi.isStatic ? null : acc.get(1),
          requireNonNull(afi.resultMethod,
              () -> "resultMethod is null. Does " + afi.declaringClass + " declare result method?"),
          acc.get(0));
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
          Comparable @Nullable [] rows;  // accessed via WinAggAddContext.compareRows
          @SuppressWarnings("nullness")
          void sample() {
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

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    @Override public boolean needCacheWhenFrameIntact() {
      return true;
    }

    @Override public Expression implementResult(AggContext info,
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
    @Override public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    @Override public boolean needCacheWhenFrameIntact() {
      return true;
    }

    @Override public Expression implementResult(AggContext info,
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

    @Override public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    @Override public boolean needCacheWhenFrameIntact() {
      return false;
    }

    @Override public Expression implementResult(AggContext info,
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
    @Override public List<Type> getStateType(AggContext info) {
      return Collections.emptyList();
    }

    @Override public void implementReset(AggContext info, AggResetContext reset) {
      // no op
    }

    @Override public void implementAdd(AggContext info, AggAddContext add) {
      // no op
    }

    @Override public boolean needCacheWhenFrameIntact() {
      return false;
    }

    @Override public Expression implementResult(AggContext info,
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
  private static class TrimImplementor extends AbstractRexCallImplementor {
    TrimImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "trim";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      final boolean strict = !translator.conformance.allowExtendedTrim();
      final Object value = translator.getLiteralValue(argValueList.get(0));
      SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag) value;
      return Expressions.call(
          BuiltInMethod.TRIM.method,
          Expressions.constant(
              flag == SqlTrimFunction.Flag.BOTH
              || flag == SqlTrimFunction.Flag.LEADING),
          Expressions.constant(
              flag == SqlTrimFunction.Flag.BOTH
              || flag == SqlTrimFunction.Flag.TRAILING),
          argValueList.get(1),
          argValueList.get(2),
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
      super(methodName, NullPolicy.STRICT, false);
      this.timestampMethod = timestampMethod;
      this.dateMethod = dateMethod;
    }

    @Override String getVariableName() {
      return "periodName";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      Expression operand = argValueList.get(0);
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
      super(methodName, NullPolicy.STRICT, false);
      this.timestampMethod = timestampMethod;
      this.dateMethod = dateMethod;
    }

    @Override String getVariableName() {
      return "floor";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      switch (call.getOperands().size()) {
      case 1:
        switch (call.getType().getSqlTypeName()) {
        case BIGINT:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          return argValueList.get(0);
        default:
          return super.implementSafe(translator, call, argValueList);
        }

      case 2:
        final Type type;
        final Method floorMethod;
        final boolean preFloor;
        Expression operand = argValueList.get(0);
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
          preFloor = true;
          break;
        default:
          type = int.class;
          floorMethod = dateMethod;
          preFloor = false;
        }
        final TimeUnitRange timeUnitRange =
            (TimeUnitRange) requireNonNull(translator.getLiteralValue(argValueList.get(1)),
            "timeUnitRange");
        switch (timeUnitRange) {
        case YEAR:
        case QUARTER:
        case MONTH:
        case WEEK:
        case DAY:
          final Expression operand1 =
              preFloor ? call(operand, type, TimeUnit.DAY) : operand;
          return Expressions.call(floorMethod,
              translator.getLiteral(argValueList.get(1)), operand1);
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
          EnumUtils.convert(operand, type),
          EnumUtils.convert(
              Expressions.constant(timeUnit.multiplier), type));
    }
  }

  /** Implementor for a function that generates calls to a given method. */
  private static class MethodImplementor extends AbstractRexCallImplementor {
    protected final Method method;

    MethodImplementor(Method method, @Nullable NullPolicy nullPolicy, boolean harmonize) {
      super(nullPolicy, harmonize);
      this.method = method;
    }

    @Override String getVariableName() {
      return "method_call";
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      if (Modifier.isStatic(method.getModifiers())) {
        return call(method, null, argValueList);
      } else {
        return call(method, argValueList.get(0), Util.skip(argValueList, 1));
      }
    }

    static Expression call(Method method, @Nullable Expression target,
        List<Expression> args) {
      if (method.isVarArgs()) {
        final int last = method.getParameterCount() - 1;
        ImmutableList<Expression> vargs = ImmutableList.<Expression>builder()
            .addAll(args.subList(0, last))
            .add(
                Expressions.newArrayInit(method.getParameterTypes()[last],
                    args.subList(last, args.size()).toArray(new Expression[0])))
            .build();
        return Expressions.call(target, method, vargs);
      } else {
        final Class<?> clazz = method.getDeclaringClass();
        return EnumUtils.call(target, clazz, method.getName(), args);
      }
    }
  }

  /** Implementor for {@link org.apache.calcite.sql.fun.SqlPosixRegexOperator}s. */
  private static class PosixRegexMethodImplementor extends MethodImplementor {
    protected final boolean caseSensitive;

    PosixRegexMethodImplementor(boolean caseSensitive) {
      super(BuiltInMethod.POSIX_REGEX.method, NullPolicy.STRICT, false);
      this.caseSensitive = caseSensitive;
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      assert argValueList.size() == 2;
      // Add extra parameter (caseSensitive boolean flag), required by SqlFunctions#posixRegex.
      final List<Expression> newOperands = new ArrayList<>(argValueList);
      newOperands.add(Expressions.constant(caseSensitive));
      return super.implementSafe(translator, call, newOperands);
    }
  }

  /**
   * Implementor for JSON_VALUE function, convert to solid format
   * "JSON_VALUE(json_doc, path, empty_behavior, empty_default, error_behavior, error default)"
   * in order to simplify the runtime implementation.
   *
   * <p>We should avoid this when we support
   * variable arguments function.
   */
  private static class JsonValueImplementor extends MethodImplementor {
    JsonValueImplementor(Method method) {
      super(method, NullPolicy.ARG0, false);
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      final Expression expression;
      final List<Expression> newOperands = new ArrayList<>();
      newOperands.add(argValueList.get(0));
      newOperands.add(argValueList.get(1));
      List<Expression> leftExprs = Util.skip(argValueList, 2);
      // Default value for JSON_VALUE behaviors.
      Expression emptyBehavior = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL);
      Expression defaultValueOnEmpty = Expressions.constant(null);
      Expression errorBehavior = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL);
      Expression defaultValueOnError = Expressions.constant(null);
      // Patched up with user defines.
      if (leftExprs.size() > 0) {
        for (int i = 0; i < leftExprs.size(); i++) {
          Expression expr = leftExprs.get(i);
          final Object exprVal = translator.getLiteralValue(expr);
          if (exprVal != null) {
            int defaultSymbolIdx = i - 2;
            if (exprVal == SqlJsonEmptyOrError.EMPTY) {
              if (defaultSymbolIdx >= 0
                  && translator.getLiteralValue(leftExprs.get(defaultSymbolIdx))
                      == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                defaultValueOnEmpty = leftExprs.get(i - 1);
                emptyBehavior = leftExprs.get(defaultSymbolIdx);
              } else {
                emptyBehavior = leftExprs.get(i - 1);
              }
            } else if (exprVal == SqlJsonEmptyOrError.ERROR) {
              if (defaultSymbolIdx >= 0
                  && translator.getLiteralValue(leftExprs.get(defaultSymbolIdx))
                      == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                defaultValueOnError = leftExprs.get(i - 1);
                errorBehavior = leftExprs.get(defaultSymbolIdx);
              } else {
                errorBehavior = leftExprs.get(i - 1);
              }
            }
          }
        }
      }
      newOperands.add(emptyBehavior);
      newOperands.add(defaultValueOnEmpty);
      newOperands.add(errorBehavior);
      newOperands.add(defaultValueOnError);
      Class clazz = method.getDeclaringClass();
      expression = EnumUtils.call(null, clazz, method.getName(), newOperands);

      final Type returnType =
          translator.typeFactory.getJavaClass(call.getType());
      return EnumUtils.convert(expression, returnType);
    }
  }

  /** Implementor for SQL functions that generates calls to a given method name.
   *
   * <p>Use this, as opposed to {@link MethodImplementor}, if the SQL function
   * is overloaded; then you can use one implementor for several overloads. */
  private static class MethodNameImplementor extends AbstractRexCallImplementor {
    protected final String methodName;

    MethodNameImplementor(String methodName,
        NullPolicy nullPolicy, boolean harmonize) {
      super(nullPolicy, harmonize);
      this.methodName = methodName;
    }

    @Override String getVariableName() {
      return "method_name_call";
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      return EnumUtils.call(null, SqlFunctions.class, methodName, argValueList);
    }
  }

  /** Implementor for binary operators. */
  private static class BinaryImplementor extends AbstractRexCallImplementor {
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

    private static final List<SqlBinaryOperator> EQUALS_OPERATORS =
        ImmutableList.of(
            SqlStdOperatorTable.EQUALS,
            SqlStdOperatorTable.NOT_EQUALS);

    public static final String METHOD_POSTFIX_FOR_ANY_TYPE = "Any";

    private final ExpressionType expressionType;
    private final String backupMethodName;

    BinaryImplementor(NullPolicy nullPolicy, boolean harmonize,
        ExpressionType expressionType, String backupMethodName) {
      super(nullPolicy, harmonize);
      this.expressionType = expressionType;
      this.backupMethodName = backupMethodName;
    }

    @Override String getVariableName() {
      return "binary_call";
    }

    @Override Expression implementSafe(
        final RexToLixTranslator translator,
        final RexCall call,
        final List<Expression> argValueList) {
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
          return callBackupMethodAnyType(argValueList);
        }

        final Type type0 = argValueList.get(0).getType();
        final Type type1 = argValueList.get(1).getType();
        final SqlBinaryOperator op = (SqlBinaryOperator) call.getOperator();
        final RelDataType relDataType0 = call.getOperands().get(0).getType();
        final Expression fieldComparator = generateCollatorExpression(relDataType0.getCollation());
        if (fieldComparator != null) {
          argValueList.add(fieldComparator);
        }
        final Primitive primitive = Primitive.ofBoxOr(type0);
        if (primitive == null
            || type1 == BigDecimal.class
            || COMPARISON_OPERATORS.contains(op)
            && !COMP_OP_TYPES.contains(primitive)) {
          return Expressions.call(SqlFunctions.class, backupMethodName,
              argValueList);
        }
        // When checking equals or not equals on two primitive boxing classes
        // (i.e. Long x, Long y), we should fall back to call `SqlFunctions.eq(x, y)`
        // or `SqlFunctions.ne(x, y)`, rather than `x == y`
        final Primitive boxPrimitive0 = Primitive.ofBox(type0);
        final Primitive boxPrimitive1 = Primitive.ofBox(type1);
        if (EQUALS_OPERATORS.contains(op)
            && boxPrimitive0 != null && boxPrimitive1 != null) {
          return Expressions.call(SqlFunctions.class, backupMethodName,
              argValueList);
        }
      }
      return Expressions.makeBinary(expressionType,
          argValueList.get(0), argValueList.get(1));
    }

    /** Returns whether any of a call's operands have ANY type. */
    private static boolean anyAnyOperands(RexCall call) {
      for (RexNode operand : call.operands) {
        if (operand.getType().getSqlTypeName() == SqlTypeName.ANY) {
          return true;
        }
      }
      return false;
    }

    private Expression callBackupMethodAnyType(List<Expression> expressions) {
      final String backupMethodNameForAnyType =
          backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE;

      // one or both of parameter(s) is(are) ANY type
      final Expression expression0 = maybeBox(expressions.get(0));
      final Expression expression1 = maybeBox(expressions.get(1));
      return Expressions.call(SqlFunctions.class, backupMethodNameForAnyType,
          expression0, expression1);
    }

    private static Expression maybeBox(Expression expression) {
      final Primitive primitive = Primitive.of(expression.getType());
      if (primitive != null) {
        expression = Expressions.box(expression, primitive);
      }
      return expression;
    }
  }

  /** Implementor for unary operators. */
  private static class UnaryImplementor extends AbstractRexCallImplementor {
    private final ExpressionType expressionType;
    private final @Nullable String backupMethodName;

    UnaryImplementor(ExpressionType expressionType, NullPolicy nullPolicy,
        @Nullable String backupMethodName) {
      super(nullPolicy, false);
      this.expressionType = expressionType;
      this.backupMethodName = backupMethodName;
    }

    @Override String getVariableName() {
      return "unary_call";
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      final Expression argValue = argValueList.get(0);

      final Expression e;
      //Special case for implementing unary minus with BigDecimal type
      //for other data type(except BigDecimal) '-' operator is OK, but for
      //BigDecimal, we should call negate method of BigDecimal
      if (expressionType == ExpressionType.Negate && argValue.type == BigDecimal.class
          && null != backupMethodName) {
        e = Expressions.call(argValue, backupMethodName);
      } else {
        e = Expressions.makeUnary(expressionType, argValue);
      }

      if (e.type.equals(argValue.type)) {
        return e;
      }
      // Certain unary operators do not preserve type. For example, the "-"
      // operator applied to a "byte" expression returns an "int".
      return Expressions.convert_(e, argValue.type);
    }
  }

  /** Implementor for the {@code EXTRACT(unit FROM datetime)} function. */
  private static class ExtractImplementor extends AbstractRexCallImplementor {
    ExtractImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "extract";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      final TimeUnitRange timeUnitRange =
          (TimeUnitRange) translator.getLiteralValue(argValueList.get(0));
      final TimeUnit unit = requireNonNull(timeUnitRange, "timeUnitRange").startUnit;
      Expression operand = argValueList.get(1);
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
              argValueList.get(0), operand);
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
        default:
          break;
        }
        break;
      case HOUR:
      case MINUTE:
      case SECOND:
        switch (sqlTypeName) {
        case DATE:
          return Expressions.multiply(operand, Expressions.constant(0L));
        default:
          break;
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

  /** Implementor for the SQL {@code COALESCE} operator. */
  private static class CoalesceImplementor extends AbstractRexCallImplementor {
    CoalesceImplementor() {
      super(NullPolicy.NONE, false);
    }

    @Override String getVariableName() {
      return "coalesce";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return implementRecurse(translator, argValueList);
    }

    private static Expression implementRecurse(RexToLixTranslator translator,
        final List<Expression> argValueList) {
      if (argValueList.size() == 1) {
        return argValueList.get(0);
      } else {
        return Expressions.condition(
            translator.checkNotNull(argValueList.get(0)),
            argValueList.get(0),
            implementRecurse(translator, Util.skip(argValueList)));
      }
    }
  }

  /** Implementor for the SQL {@code CAST} operator. */
  private static class CastImplementor extends AbstractRexCallImplementor {
    CastImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "cast";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      assert call.getOperands().size() == 1;
      final RelDataType sourceType = call.getOperands().get(0).getType();

      // Short-circuit if no cast is required
      RexNode arg = call.getOperands().get(0);
      if (call.getType().equals(sourceType)) {
        // No cast required, omit cast
        return argValueList.get(0);
      }
      if (SqlTypeUtil.equalSansNullability(translator.typeFactory,
          call.getType(), arg.getType())
          && translator.deref(arg) instanceof RexLiteral) {
        return RexToLixTranslator.translateLiteral(
            (RexLiteral) translator.deref(arg), call.getType(),
            translator.typeFactory, NullAs.NULL);
      }
      final RelDataType targetType =
          nullifyType(translator.typeFactory, call.getType(), false);
      return translator.translateCast(sourceType,
              targetType, argValueList.get(0));
    }

    private static RelDataType nullifyType(JavaTypeFactory typeFactory,
        final RelDataType type, final boolean nullable) {
      if (type instanceof RelDataTypeFactoryImpl.JavaType) {
        Class<?> javaClass = ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass();
        final Class<?> primitive = Primitive.unbox(javaClass);
        if (primitive != javaClass) {
          return typeFactory.createJavaType(primitive);
        }
      }
      return typeFactory.createTypeWithNullability(type, nullable);
    }
  }

  /** Implementor for the {@code REINTERPRET} internal SQL operator. */
  private static class ReinterpretImplementor extends AbstractRexCallImplementor {
    ReinterpretImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "reInterpret";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      assert call.getOperands().size() == 1;
      return argValueList.get(0);
    }
  }

  /** Implementor for a array concat. */
  private static class ArrayConcatImplementor extends AbstractRexCallImplementor {

    ArrayConcatImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "array_concat";
    }

    @Override Expression implementSafe(RexToLixTranslator translator, RexCall call,
        List<Expression> argValueList) {
      final BlockBuilder blockBuilder = translator.getBlockBuilder();
      final Expression list =
          blockBuilder.append("list", Expressions.new_(ArrayList.class), false);
      final Expression nullValue = Expressions.constant(null);
      for (Expression expression : argValueList) {
        blockBuilder.add(
            Expressions.ifThenElse(
                Expressions.or(
                    Expressions.equal(nullValue, list),
                    Expressions.equal(nullValue, expression)),
                Expressions.assign(list, nullValue),
                Expressions.statement(
                    Expressions.call(list, BuiltInMethod.COLLECTION_ADDALL.method, expression)))
        );
      }
      return list;
    }
  }

  /** Implementor for a array or string concat. */
  private static class ConcatImplementor extends AbstractRexCallImplementor {
    private ArrayConcatImplementor arrayConcatImplementor =
        new ArrayConcatImplementor();
    private MethodImplementor stringConcatImplementor
        = new MethodImplementor(BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT, false);
    ConcatImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "concat";
    }

    @Override Expression implementSafe(RexToLixTranslator translator, RexCall call,
        List<Expression> argValueList) {
      if (call.type.getSqlTypeName() == SqlTypeName.ARRAY) {
        return arrayConcatImplementor.implementSafe(translator, call, argValueList);
      }
      return stringConcatImplementor.implementSafe(translator, call, argValueList);
    }
  }

  /** Implementor for a value-constructor. */
  private static class ValueConstructorImplementor
      extends AbstractRexCallImplementor {

    ValueConstructorImplementor() {
      super(NullPolicy.NONE, false);
    }

    @Override String getVariableName() {
      return "value_constructor";
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      SqlKind kind = call.getOperator().getKind();
      final BlockBuilder blockBuilder = translator.getBlockBuilder();
      switch (kind) {
      case MAP_VALUE_CONSTRUCTOR:
        Expression map =
            blockBuilder.append("map", Expressions.new_(LinkedHashMap.class),
                false);
        for (int i = 0; i < argValueList.size(); i++) {
          Expression key = argValueList.get(i++);
          Expression value = argValueList.get(i);
          blockBuilder.add(
              Expressions.statement(
                  Expressions.call(map, BuiltInMethod.MAP_PUT.method,
                      Expressions.box(key), Expressions.box(value))));
        }
        return map;
      case ARRAY_VALUE_CONSTRUCTOR:
        Expression lyst =
            blockBuilder.append("list", Expressions.new_(ArrayList.class),
                false);
        for (Expression value : argValueList) {
          blockBuilder.add(
              Expressions.statement(
                  Expressions.call(lyst, BuiltInMethod.COLLECTION_ADD.method,
                      Expressions.box(value))));
        }
        return lyst;
      default:
        throw new AssertionError("unexpected: " + kind);
      }
    }
  }

  /** Implementor for the {@code ITEM} SQL operator. */
  private static class ItemImplementor extends AbstractRexCallImplementor {
    ItemImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "item";
    }

    // Since we follow PostgreSQL's semantics that an out-of-bound reference
    // returns NULL, x[y] can return null even if x and y are both NOT NULL.
    // (In SQL standard semantics, an out-of-bound reference to an array
    // throws an exception.)
    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      final MethodImplementor implementor =
          getImplementor(call.getOperands().get(0).getType().getSqlTypeName());
      return implementor.implementSafe(translator, call, argValueList);
    }

    private MethodImplementor getImplementor(SqlTypeName sqlTypeName) {
      switch (sqlTypeName) {
      case ARRAY:
        return new MethodImplementor(BuiltInMethod.ARRAY_ITEM.method, nullPolicy, false);
      case MAP:
        return new MethodImplementor(BuiltInMethod.MAP_ITEM.method, nullPolicy, false);
      default:
        return new MethodImplementor(BuiltInMethod.ANY_ITEM.method, nullPolicy, false);
      }
    }
  }

  /** Implementor for SQL system functions.
   *
   * <p>Several of these are represented internally as constant values, set
   * per execution. */
  private static class SystemFunctionImplementor
      extends AbstractRexCallImplementor {
    SystemFunctionImplementor() {
      super(NullPolicy.NONE, false);
    }

    @Override String getVariableName() {
      return "system_func";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
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

  /** Implementor for the {@code NOT} operator. */
  private static class NotImplementor extends AbstractRexCallImplementor {
    private AbstractRexCallImplementor implementor;

    private NotImplementor(AbstractRexCallImplementor implementor) {
      super(null, false);
      this.implementor = implementor;
    }

    static AbstractRexCallImplementor of(AbstractRexCallImplementor implementor) {
      return new NotImplementor(implementor);
    }

    @Override String getVariableName() {
      return "not";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      final Expression expression =
          implementor.implementSafe(translator, call, argValueList);
      return Expressions.not(expression);
    }
  }

  /** Implementor for various datetime arithmetic. */
  private static class DatetimeArithmeticImplementor
      extends AbstractRexCallImplementor {
    DatetimeArithmeticImplementor() {
      super(NullPolicy.STRICT, false);
    }
    @Override String getVariableName() {
      return "dateTime_arithmetic";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      final RexNode operand0 = call.getOperands().get(0);
      Expression trop0 = argValueList.get(0);
      final SqlTypeName typeName1 =
          call.getOperands().get(1).getType().getSqlTypeName();
      Expression trop1 = argValueList.get(1);
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
            break;
          default:
            break;
          }
        }
        break;
      case TIME:
        trop1 = Expressions.convert_(trop1, int.class);
        break;
      default:
        break;
      }
      switch (typeName1) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        switch (call.getKind()) {
        case MINUS:
          trop1 = Expressions.negate(trop1);
          break;
        default:
          break;
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
          default:
            break;
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
    private static Expression normalize(SqlTypeName typeName, Expression e) {
      switch (typeName) {
      case TIME:
        return Expressions.call(BuiltInMethod.FLOOR_MOD.method, e,
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
      default:
        return e;
      }
    }
  }

  /** Implements CLASSIFIER match-recognize function. */
  private static class ClassifierImplementor implements MatchImplementor {
    @Override public Expression implement(RexToLixTranslator translator, RexCall call,
        ParameterExpression row, ParameterExpression rows,
        ParameterExpression symbols, ParameterExpression i) {
      return EnumUtils.convert(
          Expressions.call(symbols, BuiltInMethod.LIST_GET.method, i),
          String.class);
    }
  }

  /** Implements the LAST match-recognize function. */
  private static class LastImplementor implements MatchImplementor {
    @Override public Expression implement(RexToLixTranslator translator, RexCall call,
        ParameterExpression row, ParameterExpression rows,
        ParameterExpression symbols, ParameterExpression i) {
      final RexNode node = call.getOperands().get(0);

      final String alpha = ((RexPatternFieldRef) call.getOperands().get(0)).getAlpha();

      // TODO: verify if the variable is needed
      @SuppressWarnings("unused")
      final BinaryExpression lastIndex = Expressions.subtract(
          Expressions.call(rows, BuiltInMethod.COLLECTION_SIZE.method),
          Expressions.constant(1));

      // Just take the last one, if exists
      if ("*".equals(alpha)) {
        setInputGetterIndex(translator, i);
        // Important, unbox the node / expression to avoid NullAs.NOT_POSSIBLE
        final RexPatternFieldRef ref = (RexPatternFieldRef) node;
        final RexPatternFieldRef newRef =
            new RexPatternFieldRef(ref.getAlpha(),
                ref.getIndex(),
                translator.typeFactory.createTypeWithNullability(ref.getType(),
                    true));
        final Expression expression = translator.translate(newRef, NullAs.NULL);
        setInputGetterIndex(translator, null);
        return expression;
      } else {
        // Alpha != "*" so we have to search for a specific one to find and use that, if found
        setInputGetterIndex(translator,
            Expressions.call(BuiltInMethod.MATCH_UTILS_LAST_WITH_SYMBOL.method,
                Expressions.constant(alpha), rows, symbols, i));

        // Important, unbox the node / expression to avoid NullAs.NOT_POSSIBLE
        final RexPatternFieldRef ref = (RexPatternFieldRef) node;
        final RexPatternFieldRef newRef =
            new RexPatternFieldRef(ref.getAlpha(),
                ref.getIndex(),
                translator.typeFactory.createTypeWithNullability(ref.getType(),
                    true));
        final Expression expression = translator.translate(newRef, NullAs.NULL);
        setInputGetterIndex(translator, null);
        return expression;
      }
    }

    private static void setInputGetterIndex(RexToLixTranslator translator, @Nullable Expression o) {
      requireNonNull((EnumerableMatch.PassedRowsInputGetter) translator.inputGetter,
          "inputGetter").setIndex(o);
    }
  }

  /** Null-safe implementor of {@code RexCall}s. */
  public interface RexCallImplementor {
    RexToLixTranslator.Result implement(
        RexToLixTranslator translator,
        RexCall call,
        List<RexToLixTranslator.Result> arguments);
  }

  /**
   * Abstract implementation of the {@link RexCallImplementor} interface.
   *
   * <p>It is not always safe to execute the {@link RexCall} directly due to
   * the special null arguments. Therefore, the generated code logic is
   * conditional correspondingly.
   *
   * <p>For example, {@code a + b} will generate two declaration statements:
   *
   * <blockquote>
   * <code>
   * final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b);<br>
   * final boolean xxx_isNull = xxx_value == null;
   * </code>
   * </blockquote>
   */
  private abstract static class AbstractRexCallImplementor
      implements RexCallImplementor {
    final @Nullable NullPolicy nullPolicy;
    private final boolean harmonize;

    AbstractRexCallImplementor(@Nullable NullPolicy nullPolicy, boolean harmonize) {
      this.nullPolicy = nullPolicy;
      this.harmonize = harmonize;
    }

    @Override public RexToLixTranslator.Result implement(
        final RexToLixTranslator translator,
        final RexCall call,
        final List<RexToLixTranslator.Result> arguments) {
      final List<Expression> argIsNullList = new ArrayList<>();
      final List<Expression> argValueList = new ArrayList<>();
      for (RexToLixTranslator.Result result: arguments) {
        argIsNullList.add(result.isNullVariable);
        argValueList.add(result.valueVariable);
      }
      final Expression condition = getCondition(argIsNullList);
      final ParameterExpression valueVariable =
          genValueStatement(translator, call, argValueList, condition);
      final ParameterExpression isNullVariable =
          genIsNullStatement(translator, valueVariable);
      return new RexToLixTranslator.Result(isNullVariable, valueVariable);
    }

    // Variable name facilitates reasoning about issues when necessary
    abstract String getVariableName();

    /** Figures out conditional expression according to NullPolicy. */
    Expression getCondition(final List<Expression> argIsNullList) {
      if (argIsNullList.size() == 0
          || nullPolicy == null
          || nullPolicy == NullPolicy.NONE) {
        return FALSE_EXPR;
      }
      if (nullPolicy == NullPolicy.ARG0) {
        return argIsNullList.get(0);
      }

      if (nullPolicy == NullPolicy.ALL) {
        // Condition for NullPolicy.ALL: v0 == null && v1 == null
        return Expressions.foldAnd(argIsNullList);
      }

      // Condition for regular cases: v0 == null || v1 == null
      return Expressions.foldOr(argIsNullList);
    }

    // E.g., "final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b)"
    private ParameterExpression genValueStatement(
        final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList,
        final Expression condition) {
      List<Expression> optimizedArgValueList = argValueList;
      if (harmonize) {
        optimizedArgValueList =
            harmonize(optimizedArgValueList, translator, call);
      }
      optimizedArgValueList = unboxIfNecessary(optimizedArgValueList);

      final Expression callValue =
          implementSafe(translator, call, optimizedArgValueList);

      // In general, RexCall's type is correct for code generation
      // and thus we should ensure the consistency.
      // However, for some special cases (e.g., TableFunction),
      // the implementation's type is correct, we can't convert it.
      final SqlOperator op = call.getOperator();
      final Type returnType = translator.typeFactory.getJavaClass(call.getType());
      final boolean noConvert = (returnType == null)
              || (returnType == callValue.getType())
              || (op instanceof SqlUserDefinedTableMacro)
              || (op instanceof SqlUserDefinedTableFunction);
      final Expression convertedCallValue =
              noConvert
              ? callValue
              : EnumUtils.convert(callValue, returnType);

      final Expression valueExpression =
          Expressions.condition(condition,
              getIfTrue(convertedCallValue.getType(), argValueList),
              convertedCallValue);
      final ParameterExpression value =
          Expressions.parameter(convertedCallValue.getType(),
              translator.getBlockBuilder().newName(getVariableName() + "_value"));
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, value, valueExpression));
      return value;
    }

    Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return getDefaultValue(type);
    }

    // E.g., "final boolean xxx_isNull = xxx_value == null"
    private ParameterExpression genIsNullStatement(
        final RexToLixTranslator translator, final ParameterExpression value) {
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE,
              translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
      final Expression isNullExpression = translator.checkNull(value);
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      return isNullVariable;
    }

    /** Ensures that operands have identical type. */
    private static List<Expression> harmonize(final List<Expression> argValueList,
        final RexToLixTranslator translator, final RexCall call) {
      int nullCount = 0;
      final List<RelDataType> types = new ArrayList<>();
      final RelDataTypeFactory typeFactory =
          translator.builder.getTypeFactory();
      for (RexNode operand : call.getOperands()) {
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
        return argValueList;
      }
      final RelDataType type = typeFactory.leastRestrictive(types);
      if (type == null) {
        // There is no common type. Presumably this is a binary operator with
        // asymmetric arguments (e.g. interval / integer) which is not intended
        // to be harmonized.
        return argValueList;
      }
      assert (nullCount > 0) == type.isNullable();
      final Type javaClass =
          translator.typeFactory.getJavaClass(type);
      final List<Expression> harmonizedArgValues = new ArrayList<>();
      for (Expression argValue : argValueList) {
        harmonizedArgValues.add(
            EnumUtils.convert(argValue, javaClass));
      }
      return harmonizedArgValues;
    }

    /** Under null check, it is safe to unbox the operands before entering the
     * implementor. */
    private List<Expression> unboxIfNecessary(final List<Expression> argValueList) {
      List<Expression> unboxValueList = argValueList;
      if (nullPolicy == NullPolicy.STRICT || nullPolicy == NullPolicy.ANY
          || nullPolicy == NullPolicy.SEMI_STRICT) {
        unboxValueList = argValueList.stream()
            .map(AbstractRexCallImplementor::unboxExpression)
            .collect(Collectors.toList());
      }
      if (nullPolicy == NullPolicy.ARG0 && argValueList.size() > 0) {
        final Expression unboxArg0 = unboxExpression(unboxValueList.get(0));
        unboxValueList.set(0, unboxArg0);
      }
      return unboxValueList;
    }

    private static Expression unboxExpression(final Expression argValue) {
      Primitive fromBox = Primitive.ofBox(argValue.getType());
      if (fromBox == null || fromBox == Primitive.VOID) {
        return argValue;
      }
      // Optimization: for "long x";
      // "Long.valueOf(x)" generates "x"
      if (argValue instanceof MethodCallExpression) {
        MethodCallExpression mce = (MethodCallExpression) argValue;
        if (mce.method.getName().equals("valueOf") && mce.expressions.size() == 1) {
          Expression originArg = mce.expressions.get(0);
          if (Primitive.of(originArg.type) == fromBox) {
            return originArg;
          }
        }
      }
      return NullAs.NOT_POSSIBLE.handle(argValue);
    }

    abstract Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList);
  }

  /**
   * Implementor for the {@code AND} operator.
   *
   * <p>If any of the arguments are false, result is false;
   * else if any arguments are null, result is null;
   * else true.
   */
  private static class LogicalAndImplementor extends AbstractRexCallImplementor {
    LogicalAndImplementor() {
      super(NullPolicy.NONE, true);
    }

    @Override String getVariableName() {
      return "logical_and";
    }

    @Override public RexToLixTranslator.Result implement(final RexToLixTranslator translator,
        final RexCall call, final List<RexToLixTranslator.Result> arguments) {
      final List<Expression> argIsNullList = new ArrayList<>();
      for (RexToLixTranslator.Result result: arguments) {
        argIsNullList.add(result.isNullVariable);
      }
      final List<Expression> nullAsTrue =
          arguments.stream()
              .map(result ->
                  Expressions.condition(result.isNullVariable, TRUE_EXPR,
                      result.valueVariable))
              .collect(Collectors.toList());
      final Expression hasFalse =
          Expressions.not(Expressions.foldAnd(nullAsTrue));
      final Expression hasNull = Expressions.foldOr(argIsNullList);
      final Expression callExpression =
          Expressions.condition(hasFalse, BOXED_FALSE_EXPR,
              Expressions.condition(hasNull, NULL_EXPR, BOXED_TRUE_EXPR));
      final RexImpTable.NullAs nullAs = translator.isNullable(call)
          ? RexImpTable.NullAs.NULL : RexImpTable.NullAs.NOT_POSSIBLE;
      final Expression valueExpression = nullAs.handle(callExpression);
      final ParameterExpression valueVariable =
          Expressions.parameter(valueExpression.getType(),
              translator.getBlockBuilder().newName(getVariableName() + "_value"));
      final Expression isNullExpression = translator.checkNull(valueVariable);
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE,
              translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      return new RexToLixTranslator.Result(isNullVariable, valueVariable);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      throw new IllegalStateException("This implementSafe should not be called,"
          + " please call implement(...)");
    }
  }

  /**
   * Implementor for the {@code OR} operator.
   *
   * <p>If any of the arguments are true, result is true;
   * else if any arguments are null, result is null;
   * else false.
   */
  private static class LogicalOrImplementor extends AbstractRexCallImplementor {
    LogicalOrImplementor() {
      super(NullPolicy.NONE, true);
    }

    @Override String getVariableName() {
      return "logical_or";
    }

    @Override public RexToLixTranslator.Result implement(final RexToLixTranslator translator,
        final RexCall call, final List<RexToLixTranslator.Result> arguments) {
      final List<Expression> argIsNullList = new ArrayList<>();
      for (RexToLixTranslator.Result result: arguments) {
        argIsNullList.add(result.isNullVariable);
      }
      final List<Expression> nullAsFalse =
          arguments.stream()
              .map(result ->
                  Expressions.condition(result.isNullVariable, FALSE_EXPR,
                      result.valueVariable))
              .collect(Collectors.toList());
      final Expression hasTrue = Expressions.foldOr(nullAsFalse);
      final Expression hasNull = Expressions.foldOr(argIsNullList);
      final Expression callExpression =
          Expressions.condition(hasTrue, BOXED_TRUE_EXPR,
              Expressions.condition(hasNull, NULL_EXPR, BOXED_FALSE_EXPR));
      final RexImpTable.NullAs nullAs = translator.isNullable(call)
          ? RexImpTable.NullAs.NULL : RexImpTable.NullAs.NOT_POSSIBLE;
      final Expression valueExpression = nullAs.handle(callExpression);
      final ParameterExpression valueVariable =
          Expressions.parameter(valueExpression.getType(),
              translator.getBlockBuilder().newName(getVariableName() + "_value"));
      final Expression isNullExpression = translator.checkNull(valueExpression);
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE,
              translator.getBlockBuilder().newName(getVariableName() + "_isNull"));
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
      translator.getBlockBuilder().add(
          Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      return new RexToLixTranslator.Result(isNullVariable, valueVariable);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      throw new IllegalStateException("This implementSafe should not be called,"
          + " please call implement(...)");
    }
  }

  /**
   * Implementor for the {@code NOT} operator.
   *
   * <p>If any of the arguments are false, result is true;
   * else if any arguments are null, result is null;
   * else false.
   */
  private static class LogicalNotImplementor extends AbstractRexCallImplementor {
    LogicalNotImplementor() {
      super(NullPolicy.NONE, true);
    }

    @Override String getVariableName() {
      return "logical_not";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.call(BuiltInMethod.NOT.method, argValueList);
    }
  }

  /**
   * Implementation that calls a given {@link java.lang.reflect.Method}.
   *
   * <p>When method is not static, a new instance of the required class is
   * created.
   */
  private static class ReflectiveImplementor extends AbstractRexCallImplementor {
    protected final Method method;

    ReflectiveImplementor(Method method, @Nullable NullPolicy nullPolicy) {
      super(nullPolicy, false);
      this.method = method;
    }

    @Override String getVariableName() {
      return "reflective_" + method.getName();
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      List<Expression> argValueList0 =
          EnumUtils.fromInternal(method.getParameterTypes(), argValueList);
      if ((method.getModifiers() & Modifier.STATIC) != 0) {
        return Expressions.call(method, argValueList0);
      } else {
        // The UDF class must have a public zero-args constructor.
        // Assume that the validator checked already.
        final Expression target = Expressions.new_(method.getDeclaringClass());
        return Expressions.call(target, method, argValueList0);
      }
    }
  }

  /** Implementor for the {@code RAND} function. */
  private static class RandImplementor extends AbstractRexCallImplementor {
    private final AbstractRexCallImplementor[] implementors = {
        new ReflectiveImplementor(BuiltInMethod.RAND.method, nullPolicy),
        new ReflectiveImplementor(BuiltInMethod.RAND_SEED.method, nullPolicy)
    };

    RandImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "rand";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return implementors[call.getOperands().size()]
          .implementSafe(translator, call, argValueList);
    }
  }

  /** Implementor for the {@code RAND_INTEGER} function. */
  private static class RandIntegerImplementor extends AbstractRexCallImplementor {
    private final AbstractRexCallImplementor[] implementors = {
        new ReflectiveImplementor(BuiltInMethod.RAND_INTEGER.method, nullPolicy),
        new ReflectiveImplementor(BuiltInMethod.RAND_INTEGER_SEED.method, nullPolicy)
    };

    RandIntegerImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "rand_integer";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return implementors[call.getOperands().size() - 1]
          .implementSafe(translator, call, argValueList);
    }
  }

  /** Implementor for the {@code PI} operator. */
  private static class PiImplementor extends AbstractRexCallImplementor {
    PiImplementor() {
      super(NullPolicy.NONE, false);
    }

    @Override String getVariableName() {
      return "pi";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.constant(Math.PI);
    }
  }

  /** Implementor for the {@code IS FALSE} SQL operator. */
  private static class IsFalseImplementor extends AbstractRexCallImplementor {
    IsFalseImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_false";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(false, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.equal(argValueList.get(0), FALSE_EXPR);
    }
  }

  /** Implementor for the {@code IS NOT FALSE} SQL operator. */
  private static class IsNotFalseImplementor extends AbstractRexCallImplementor {
    IsNotFalseImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_not_false";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(true, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.notEqual(argValueList.get(0), FALSE_EXPR);
    }
  }

  /** Implementor for the {@code IS NOT NULL} SQL operator. */
  private static class IsNotNullImplementor extends AbstractRexCallImplementor {
    IsNotNullImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_not_null";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(false, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.notEqual(argValueList.get(0), NULL_EXPR);
    }
  }

  /** Implementor for the {@code IS NOT TRUE} SQL operator. */
  private static class IsNotTrueImplementor extends AbstractRexCallImplementor {
    IsNotTrueImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_not_true";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(true, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.notEqual(argValueList.get(0), TRUE_EXPR);
    }
  }

  /** Implementor for the {@code IS NULL} SQL operator. */
  private static class IsNullImplementor extends AbstractRexCallImplementor {
    IsNullImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_null";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(true, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.equal(argValueList.get(0), NULL_EXPR);
    }
  }

  /** Implementor for the {@code IS TRUE} SQL operator. */
  private static class IsTrueImplementor extends AbstractRexCallImplementor {
    IsTrueImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "is_true";
    }

    @Override Expression getIfTrue(Type type, final List<Expression> argValueList) {
      return Expressions.constant(false, type);
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.equal(argValueList.get(0), TRUE_EXPR);
    }
  }

  /** Implementor for the {@code REGEXP_REPLACE} function. */
  private static class RegexpReplaceImplementor extends AbstractRexCallImplementor {
    private final AbstractRexCallImplementor[] implementors = {
        new ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE3.method, nullPolicy),
        new ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE4.method, nullPolicy),
        new ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE5.method, nullPolicy),
        new ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE6.method, nullPolicy),
    };

    RegexpReplaceImplementor() {
      super(NullPolicy.STRICT, false);
    }

    @Override String getVariableName() {
      return "regexp_replace";
    }

    @Override Expression implementSafe(RexToLixTranslator translator,
        RexCall call, List<Expression> argValueList) {
      return implementors[call.getOperands().size() - 3]
          .implementSafe(translator, call, argValueList);
    }
  }

  /** Implementor for the {@code DEFAULT} function. */
  private static class DefaultImplementor extends AbstractRexCallImplementor {
    DefaultImplementor() {
      super(NullPolicy.NONE, false);
    }

    @Override String getVariableName() {
      return "default";
    }

    @Override Expression implementSafe(final RexToLixTranslator translator,
        final RexCall call, final List<Expression> argValueList) {
      return Expressions.constant(null);
    }
  }

  /** Implements the {@code TUMBLE} table function. */
  private static class TumbleImplementor implements TableFunctionCallImplementor {
    @Override public Expression implement(RexToLixTranslator translator,
        Expression inputEnumerable,
        RexCall call, PhysType inputPhysType, PhysType outputPhysType) {
      // The table operand is removed from the RexCall because it
      // represents the input, see StandardConvertletTable#convertWindowFunction.
      Expression intervalExpression = translator.translate(call.getOperands().get(1));
      RexCall descriptor = (RexCall) call.getOperands().get(0);
      final ParameterExpression parameter =
          Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()),
              "_input");
      Expression wmColExpr =
          inputPhysType.fieldReference(parameter,
              ((RexInputRef) descriptor.getOperands().get(0)).getIndex(),
              outputPhysType.getJavaFieldType(
                  inputPhysType.getRowType().getFieldCount()));

      // handle the optional offset parameter. Use 0 for the default value when offset
      // parameter is not set.
      final Expression offsetExpr = call.getOperands().size() > 2
          ? translator.translate(call.getOperands().get(2))
          : Expressions.constant(0, long.class);

      return Expressions.call(
          BuiltInMethod.TUMBLING.method,
          inputEnumerable,
          EnumUtils.tumblingWindowSelector(
              inputPhysType,
              outputPhysType,
              wmColExpr,
              intervalExpression,
              offsetExpr));
    }
  }

  /** Implements the {@code HOP} table function. */
  private static class HopImplementor implements TableFunctionCallImplementor {
    @Override public Expression implement(RexToLixTranslator translator,
        Expression inputEnumerable, RexCall call, PhysType inputPhysType, PhysType outputPhysType) {
      Expression slidingInterval = translator.translate(call.getOperands().get(1));
      Expression windowSize = translator.translate(call.getOperands().get(2));
      RexCall descriptor = (RexCall) call.getOperands().get(0);
      Expression wmColIndexExpr =
          Expressions.constant(((RexInputRef) descriptor.getOperands().get(0)).getIndex());

      // handle the optional offset parameter. Use 0 for the default value when offset
      // parameter is not set.
      final Expression offsetExpr = call.getOperands().size() > 3
          ? translator.translate(call.getOperands().get(3))
          : Expressions.constant(0, long.class);

      return Expressions.call(
          BuiltInMethod.HOPPING.method,
          Expressions.list(
              Expressions.call(inputEnumerable,
                  BuiltInMethod.ENUMERABLE_ENUMERATOR.method),
              wmColIndexExpr,
              slidingInterval,
              windowSize,
              offsetExpr));
    }
  }

  /** Implements the {@code SESSION} table function. */
  private static class SessionImplementor implements TableFunctionCallImplementor {
    @Override public Expression implement(RexToLixTranslator translator,
        Expression inputEnumerable, RexCall call, PhysType inputPhysType, PhysType outputPhysType) {
      RexCall timestampDescriptor = (RexCall) call.getOperands().get(0);
      RexCall keyDescriptor = (RexCall) call.getOperands().get(1);
      Expression gapInterval = translator.translate(call.getOperands().get(2));

      Expression wmColIndexExpr =
          Expressions.constant(((RexInputRef) timestampDescriptor.getOperands().get(0)).getIndex());
      Expression keyColIndexExpr =
          Expressions.constant(((RexInputRef) keyDescriptor.getOperands().get(0)).getIndex());

      return Expressions.call(BuiltInMethod.SESSIONIZATION.method,
          Expressions.list(
              Expressions.call(inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method),
              wmColIndexExpr,
              keyColIndexExpr,
              gapInterval));
    }
  }
}
