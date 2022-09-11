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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.jts.geom.Geometry;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OCTET_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PREV;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
  public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
      ImmutableMap.<Method, SqlOperator>builder()
          .put(findMethod(String.class, "toUpperCase"), UPPER)
          .put(BuiltInMethod.SUBSTRING.method, SUBSTRING)
          .put(BuiltInMethod.OCTET_LENGTH.method, OCTET_LENGTH)
          .put(BuiltInMethod.CHAR_LENGTH.method, CHAR_LENGTH)
          .put(BuiltInMethod.TRANSLATE3.method, TRANSLATE3)
          .build();

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final @Nullable RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  final RexToLixTranslator.@Nullable InputGetter inputGetter;
  private final BlockBuilder list;
  private final @Nullable BlockBuilder staticList;
  private final @Nullable Function1<String, InputGetter> correlates;

  /**
   * Map from RexLiteral's variable name to its literal, which is often a
   * ({@link org.apache.calcite.linq4j.tree.ConstantExpression}))
   * It is used in the some {@code RexCall}'s implementors, such as
   * {@code ExtractImplementor}.
   *
   * @see #getLiteral
   * @see #getLiteralValue
   */
  private final Map<Expression, Expression> literalMap = new HashMap<>();

  /** For {@code RexCall}, keep the list of its operand's {@code Result}.
   * It is useful when creating a {@code CallImplementor}. */
  private final Map<RexCall, List<Result>> callOperandResultMap =
      new HashMap<>();

  /** Map from RexNode under specific storage type to its Result, to avoid
   * generating duplicate code. For {@code RexInputRef}, {@code RexDynamicParam}
   * and {@code RexFieldAccess}. */
  private final Map<Pair<RexNode, @Nullable Type>, Result> rexWithStorageTypeResultMap =
      new HashMap<>();

  /** Map from RexNode to its Result, to avoid generating duplicate code.
   * For {@code RexLiteral} and {@code RexCall}. */
  private final Map<RexNode, Result> rexResultMap = new HashMap<>();

  private @Nullable Type currentStorageType;

  private static Method findMethod(
      Class<?> clazz, String name, Class... parameterTypes) {
    try {
      return clazz.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private RexToLixTranslator(@Nullable RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      @Nullable InputGetter inputGetter,
      BlockBuilder list,
      @Nullable BlockBuilder staticList,
      RexBuilder builder,
      SqlConformance conformance,
      @Nullable Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = requireNonNull(typeFactory, "typeFactory");
    this.conformance = requireNonNull(conformance, "conformance");
    this.root = requireNonNull(root, "root");
    this.inputGetter = inputGetter;
    this.list = requireNonNull(list, "list");
    this.staticList = staticList;
    this.builder = requireNonNull(builder, "builder");
    this.correlates = correlates; // may be null
  }

  /**
   * Translates a {@link RexProgram} to a sequence of expressions and
   * declarations.
   *
   * @param program Program to be translated
   * @param typeFactory Type factory
   * @param conformance SQL conformance
   * @param list List of statements, populated with declarations
   * @param staticList List of member declarations
   * @param outputPhysType Output type, or null
   * @param root Root expression
   * @param inputGetter Generates expressions for inputs
   * @param correlates Provider of references to the values of correlated
   *                   variables
   * @return Sequence of expressions, optional condition
   */
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, @Nullable BlockBuilder staticList,
      @Nullable PhysType outputPhysType, Expression root,
      InputGetter inputGetter, @Nullable Function1<String, InputGetter> correlates) {
    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, staticList, new RexBuilder(typeFactory), conformance,  null)
        .setCorrelates(correlates)
        .translateList(program.getProjectList(), storageTypes);
  }

  @Deprecated // to be removed before 2.0
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, @Nullable PhysType outputPhysType, Expression root,
      InputGetter inputGetter, @Nullable Function1<String, InputGetter> correlates) {
    return translateProjects(program, typeFactory, conformance, list, null,
        outputPhysType, root, inputGetter, correlates);
  }

  public static Expression translateTableFunction(JavaTypeFactory typeFactory,
      SqlConformance conformance, BlockBuilder list,
      Expression root, RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    return new RexToLixTranslator(null, typeFactory, root, null, list,
        null, new RexBuilder(typeFactory), conformance, null)
        .translateTableFunction(rexCall, inputEnumerable, inputPhysType, outputPhysType);
  }

  /** Creates a translator for translating aggregate functions. */
  public static RexToLixTranslator forAggregation(JavaTypeFactory typeFactory,
      BlockBuilder list, @Nullable InputGetter inputGetter, SqlConformance conformance) {
    final ParameterExpression root = DataContext.ROOT;
    return new RexToLixTranslator(null, typeFactory, root, inputGetter, list,
        null, new RexBuilder(typeFactory), conformance, null);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, @Nullable Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      @Nullable Type storageType) {
    currentStorageType = storageType;
    final Result result = expr.accept(this);
    final Expression translated =
        requireNonNull(EnumUtils.toInternal(result.valueVariable, storageType));
    // When we asked for not null input that would be stored as box, avoid unboxing
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
        && translated.type.equals(storageType)) {
      return translated;
    }
    return nullAs.handle(translated);
  }

  Expression translateCast(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    Expression convert = null;
    switch (targetType.getSqlTypeName()) {
    case ANY:
      convert = operand;
      break;
    case GEOMETRY:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(BuiltInMethod.ST_GEOM_FROM_EWKT.method, operand);
        break;
      default:
        break;
      }
      break;
    case DATE:
      convert = translateCastToDate(sourceType, operand);
      break;
    case TIME:
      convert = translateCastToTime(sourceType, operand);
      break;
    case TIME_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand);
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIME_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
                operand));
        break;
      default:
        break;
      }
      break;
    case TIMESTAMP:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(BuiltInMethod.STRING_TO_TIMESTAMP.method, operand);
        break;
      case DATE:
        convert = Expressions.multiply(
            Expressions.convert_(operand, long.class),
            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));
        break;
      case TIME:
        convert =
            Expressions.add(
                Expressions.multiply(
                    Expressions.convert_(
                        Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                        long.class),
                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                Expressions.convert_(operand, long.class));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      default:
        break;
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(
                BuiltInMethod.STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                operand);
        break;
      case DATE:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.multiply(
                        Expressions.convert_(operand, long.class),
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)))),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    Expressions.add(
                        Expressions.multiply(
                            Expressions.convert_(
                                Expressions.call(BuiltInMethod.CURRENT_DATE.method, root),
                                long.class),
                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                        Expressions.convert_(operand, long.class)))),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                Expressions.call(
                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                    Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                operand));
        break;
      case TIMESTAMP:
        convert = Expressions.call(
            BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
            RexImpTable.optimize2(
                operand,
                Expressions.call(
                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                    operand)),
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root));
        break;
      default:
        break;
      }
      break;
    case BOOLEAN:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert = Expressions.call(
            BuiltInMethod.STRING_TO_BOOLEAN.method,
            operand);
        break;
      default:
        break;
      }
      break;
    case CHAR:
    case VARCHAR:
      final SqlIntervalQualifier interval =
          sourceType.getIntervalQualifier();
      switch (sourceType.getSqlTypeName()) {
      case DATE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_DATE_TO_STRING.method,
                operand));
        break;
      case TIME:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIME_TO_STRING.method,
                operand));
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case TIMESTAMP:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                operand));
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
                operand,
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
        break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                operand,
                Expressions.constant(requireNonNull(interval, "interval").timeUnitRange)));
        break;
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
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                operand,
                Expressions.constant(requireNonNull(interval, "interval").timeUnitRange),
                Expressions.constant(
                    interval.getFractionalSecondPrecision(
                        typeFactory.getTypeSystem()))));
        break;
      case BOOLEAN:
        convert = RexImpTable.optimize2(
            operand,
            Expressions.call(
                BuiltInMethod.BOOLEAN_TO_STRING.method,
                operand));
        break;
      default:
        break;
      }
      break;
    default:
      break;
    }
    if (convert == null) {
      convert = EnumUtils.convert(operand, typeFactory.getJavaClass(targetType));
    }
    // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
    // longer than n.
    boolean pad = false;
    boolean truncate = true;
    switch (targetType.getSqlTypeName()) {
    case CHAR:
    case BINARY:
      pad = true;
      // fall through
    case VARCHAR:
    case VARBINARY:
      final int targetPrecision = targetType.getPrecision();
      if (targetPrecision >= 0) {
        switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
          // If this is a widening cast, no need to truncate.
          final int sourcePrecision = sourceType.getPrecision();
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              <= 0) {
            truncate = false;
          }
          // If this is a widening cast, no need to pad.
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              >= 0) {
            pad = false;
          }
          // fall through
        default:
          if (truncate || pad) {
            convert =
                Expressions.call(
                    pad
                        ? BuiltInMethod.TRUNCATE_OR_PAD.method
                        : BuiltInMethod.TRUNCATE.method,
                    convert,
                    Expressions.constant(targetPrecision));
          }
        }
      }
      break;
    case TIMESTAMP:
      int targetScale = targetType.getScale();
      if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
        targetScale = 0;
      }
      if (targetScale < sourceType.getScale()) {
        convert =
            Expressions.call(
                BuiltInMethod.ROUND_LONG.method,
                convert,
                Expressions.constant(
                    (long) Math.pow(10, 3 - targetScale)));
      }
      break;
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
      switch (requireNonNull(sourceType.getSqlTypeName().getFamily(),
          () -> "null SqlTypeFamily for " + sourceType + ", SqlTypeName "
              + sourceType.getSqlTypeName())) {
      case NUMERIC:
        final BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;
        final BigDecimal divider = BigDecimal.ONE;
        convert = RexImpTable.multiplyDivide(convert, multiplier, divider);
        break;
      default:
        break;
      }
      break;
    default:
      break;
    }
    return scaleIntervalToNumber(sourceType, targetType, convert);
  }

  private @Nullable Expression translateCastToTime(RelDataType sourceType, Expression operand) {
    Expression convert = null;
    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      convert =
          Expressions.call(BuiltInMethod.STRING_TO_TIME.method, operand);
      break;
    case TIME_WITH_LOCAL_TIME_ZONE:
      convert = RexImpTable.optimize2(
          operand,
          Expressions.call(
              BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      break;
    case TIMESTAMP:
      convert = Expressions.convert_(
          Expressions.call(
              BuiltInMethod.FLOOR_MOD.method,
              operand,
              Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
          int.class);
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      convert = RexImpTable.optimize2(
          operand,
          Expressions.call(
              BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      break;
    default:
      break;
    }
    return convert;
  }

  private @Nullable Expression translateCastToDate(RelDataType sourceType, Expression operand) {
    Expression convert = null;
    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      convert =
          Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand);
      break;
    case TIMESTAMP:
      convert = Expressions.convert_(
          Expressions.call(BuiltInMethod.FLOOR_DIV.method,
              operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
          int.class);
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      convert = RexImpTable.optimize2(
          operand,
          Expressions.call(
              BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
              operand,
              Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));
      break;
    default:
      break;
    }
    return convert;
  }

  /**
   * Handle checked Exceptions declared in Method. In such case,
   * method call should be wrapped in a try...catch block.
   * "
   *      final Type method_call;
   *      try {
   *        method_call = callExpr
   *      } catch (Exception e) {
   *        throw new RuntimeException(e);
   *      }
   * "
   */
  Expression handleMethodCheckedExceptions(Expression callExpr) {
    // Try statement
    ParameterExpression methodCall = Expressions.parameter(
        callExpr.getType(), list.newName("method_call"));
    list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
    Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
    // Catch Block, wrap checked exception in unchecked exception
    ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
    Expression uncheckedException = Expressions.new_(RuntimeException.class, e);
    CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedException));
    list.add(Expressions.tryCatch(st, cb));
    return methodCall;
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = requireNonNull(program, "program")
          .getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a literal.
   *
   * @throws ControlFlowException if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw new ControlFlowException();
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      default:
        break;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    final Object value2;
    switch (literal.getType().getSqlTypeName()) {
    case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class,
          Expressions.constant(
              requireNonNull(bd,
                  () -> "value for " + literal).toString()));
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value2 = literal.getValueAs(Integer.class);
      javaClass = int.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
      value2 = literal.getValueAs(Long.class);
      javaClass = long.class;
      break;
    case CHAR:
    case VARCHAR:
      value2 = literal.getValueAs(String.class);
      break;
    case BINARY:
    case VARBINARY:
      return Expressions.new_(
          ByteString.class,
          Expressions.constant(
              literal.getValueAs(byte[].class),
              byte[].class));
    case GEOMETRY:
      final Geometry geom = requireNonNull(literal.getValueAs(Geometry.class),
          () -> "getValueAs(Geometries.Geom) for " + literal);
      final String wkt = SpatialTypeFunctions.ST_AsWKT(geom);
      return Expressions.call(null, BuiltInMethod.ST_GEOM_FROM_EWKT.method,
          Expressions.constant(wkt));
    case SYMBOL:
      value2 = requireNonNull(literal.getValueAs(Enum.class),
          () -> "getValueAs(Enum.class) for " + literal);
      javaClass = value2.getClass();
      break;
    default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value = literal.getValueAs(Comparable.class);
      if (primitive != null && value instanceof Number) {
        value2 = primitive.number((Number) value);
      } else {
        value2 = value;
      }
    }
    return Expressions.constant(value2, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
        EnumUtils.internalTypes(operandList));
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends @Nullable Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends @Nullable Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, EnumUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList,
      @Nullable List<? extends @Nullable Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

  private Expression translateTableFunction(RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    assert rexCall.getOperator() instanceof SqlWindowTableFunction;
    TableFunctionCallImplementor implementor =
        RexImpTable.INSTANCE.get((SqlWindowTableFunction) rexCall.getOperator());
    if (implementor == null) {
      throw Util.needToImplement("implementor of " + rexCall.getOperator().getName());
    }
    return implementor.implement(
        this, inputEnumerable, rexCall, inputPhysType, outputPhysType);
  }

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance) {
    RexLocalRef condition = program.getCondition();
    if (condition == null) {
      return RexImpTable.TRUE_EXPR;
    }
    final ParameterExpression root = DataContext.ROOT;
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            null, new RexBuilder(typeFactory), conformance, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        condition,
        RexImpTable.NullAs.FALSE);
  }

  /** Returns whether an expression is nullable.
   * @param e Expression
   * @return Whether expression is nullable
   */
  public boolean isNullable(RexNode e) {
    return e.getType().isNullable();
  }

  public RexToLixTranslator setBlock(BlockBuilder list) {
    if (list == this.list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        staticList, builder, conformance, correlates);
  }

  public RexToLixTranslator setCorrelates(
      @Nullable Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        staticList, builder, conformance, correlates);
  }

  public Expression getRoot() {
    return root;
  }

  private static Expression scaleIntervalToNumber(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    switch (requireNonNull(targetType.getSqlTypeName().getFamily(),
        () -> "SqlTypeFamily for " + targetType)) {
    case NUMERIC:
      switch (sourceType.getSqlTypeName()) {
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
        // Scale to the given field.
        final BigDecimal multiplier = BigDecimal.ONE;
        final BigDecimal divider =
            sourceType.getSqlTypeName().getEndUnit().multiplier;
        return RexImpTable.multiplyDivide(operand, multiplier, divider);
      default:
        break;
      }
      break;
    default:
      break;
    }
    return operand;
  }

  /**
   * Visit {@code RexInputRef}. If it has never been visited
   * under current storage type before, {@code RexToLixTranslator}
   * generally produces three lines of code.
   * For example, when visiting a column (named commission) in
   * table Employee, the generated code snippet is:
   * {@code
   *   final Employee current =(Employee) inputEnumerator.current();
       final Integer input_value = current.commission;
       final boolean input_isNull = input_value == null;
   * }
   */
  @Override public Result visitInputRef(RexInputRef inputRef) {
    final Pair<RexNode, @Nullable Type> key = Pair.of(inputRef, currentStorageType);
    // If the RexInputRef has been visited under current storage type already,
    // it is not necessary to visit it again, just return the result.
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    // Generate one line of code to get the input, e.g.,
    // "final Employee current =(Employee) inputEnumerator.current();"
    final Expression valueExpression = requireNonNull(inputGetter, "inputGetter").field(
        list, inputRef.getIndex(), currentStorageType);

    // Generate one line of code for the value of RexInputRef, e.g.,
    // "final Integer input_value = current.commission;"
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(), list.newName("input_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexInputRef is null, e.g.,
    // "final boolean input_isNull = input_value == null;"
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    final Result result = new Result(isNullVariable, valueVariable);

    // Cache <RexInputRef, currentStorageType>'s result
    // Note: EnumerableMatch's PrevInputGetter changes index each time,
    // it is not right to reuse the result under such case.
    if (!(inputGetter instanceof EnumerableMatch.PrevInputGetter)) {
      rexWithStorageTypeResultMap.put(key, result);
    }
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLocalRef(RexLocalRef localRef) {
    return deref(localRef).accept(this);
  }

  /**
   * Visit {@code RexLiteral}. If it has never been visited before,
   * {@code RexToLixTranslator} will generate two lines of code. For example,
   * when visiting a primitive int (10), the generated code snippet is:
   * {@code
   *   final int literal_value = 10;
   *   final boolean literal_isNull = false;
   * }
   */
  @Override public Result visitLiteral(RexLiteral literal) {
    // If the RexLiteral has been visited already, just return the result
    if (rexResultMap.containsKey(literal)) {
      return rexResultMap.get(literal);
    }
    // Generate one line of code for the value of RexLiteral, e.g.,
    // "final int literal_value = 10;"
    final Expression valueExpression = literal.isNull()
        // Note: even for null literal, we can't loss its type information
        ? getTypedNullLiteral(literal)
        : translateLiteral(literal, literal.getType(),
            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE);
    final ParameterExpression valueVariable;
    final Expression literalValue =
        appendConstant("literal_value", valueExpression);
    if (literalValue instanceof ParameterExpression) {
      valueVariable = (ParameterExpression) literalValue;
    } else {
      valueVariable =
          Expressions.parameter(valueExpression.getType(),
              list.newName("literal_value"));
      list.add(
          Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    }

    // Generate one line of code to check whether RexLiteral is null, e.g.,
    // "final boolean literal_isNull = false;"
    final Expression isNullExpression =
        literal.isNull() ? RexImpTable.TRUE_EXPR : RexImpTable.FALSE_EXPR;
    final ParameterExpression isNullVariable = Expressions.parameter(
        Boolean.TYPE, list.newName("literal_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    // Maintain the map from valueVariable (ParameterExpression) to real Expression
    literalMap.put(valueVariable, valueExpression);
    final Result result = new Result(isNullVariable, valueVariable);
    // Cache RexLiteral's result
    rexResultMap.put(literal, result);
    return result;
  }

  /**
   * Returns an {@code Expression} for null literal without losing its type
   * information.
   */
  private ConstantExpression getTypedNullLiteral(RexLiteral literal) {
    assert literal.isNull();
    Type javaClass = typeFactory.getJavaClass(literal.getType());
    switch (literal.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      javaClass = Integer.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
      javaClass = Long.class;
      break;
    default:
      break;
    }
    return javaClass == null || javaClass == Void.class
        ? RexImpTable.NULL_EXPR
        : Expressions.constant(null, javaClass);
  }

  /**
   * Visit {@code RexCall}. For most {@code SqlOperator}s, we can get the implementor
   * from {@code RexImpTable}. Several operators (e.g., CaseWhen) with special semantics
   * need to be implemented separately.
   */
  @Override public Result visitCall(RexCall call) {
    if (rexResultMap.containsKey(call)) {
      return rexResultMap.get(call);
    }
    final SqlOperator operator = call.getOperator();
    if (operator == PREV) {
      return implementPrev(call);
    }
    if (operator == CASE) {
      return implementCaseWhen(call);
    }
    if (operator == SEARCH) {
      return RexUtil.expandSearch(builder, program, call).accept(this);
    }
    final RexImpTable.RexCallImplementor implementor =
        RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    final List<RexNode> operandList = call.getOperands();
    final List<@Nullable Type> storageTypes = EnumUtils.internalTypes(operandList);
    final List<Result> operandResults = new ArrayList<>();
    for (int i = 0; i < operandList.size(); i++) {
      final Result operandResult =
          implementCallOperand(operandList.get(i), storageTypes.get(i), this);
      operandResults.add(operandResult);
    }
    callOperandResultMap.put(call, operandResults);
    final Result result = implementor.implement(this, call, operandResults);
    rexResultMap.put(call, result);
    return result;
  }

  private static Result implementCallOperand(final RexNode operand,
      final @Nullable Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    Result operandResult = operand.accept(translator);
    if (storageType != null) {
      operandResult = translator.toInnerStorageType(operandResult, storageType);
    }
    translator.currentStorageType = originalStorageType;
    return operandResult;
  }

  private static Expression implementCallOperand2(final RexNode operand,
      final @Nullable Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    final Expression result =  translator.translate(operand);
    translator.currentStorageType = originalStorageType;
    return result;
  }

  /**
   * For {@code PREV} operator, the offset of {@code inputGetter}
   * should be set first.
   */
  private Result implementPrev(RexCall call) {
    final RexNode node = call.getOperands().get(0);
    final RexNode offset = call.getOperands().get(1);
    final Expression offs = Expressions.multiply(translate(offset),
            Expressions.constant(-1));
    requireNonNull((EnumerableMatch.PrevInputGetter) inputGetter, "inputGetter")
        .setOffset(offs);
    return node.accept(this);
  }

  /**
   * The CASE operator is SQL’s way of handling if/then logic.
   * Different with other {@code RexCall}s, it is not safe to
   * implement its operands first.
   * For example: {@code
   *   select case when s=0 then false
   *          else 100/s > 0 end
   *   from (values (1),(0)) ax(s);
   * }
   */
  private Result implementCaseWhen(RexCall call) {
    final Type returnType = typeFactory.getJavaClass(call.getType());
    final ParameterExpression valueVariable =
        Expressions.parameter(returnType,
            list.newName("case_when_value"));
    list.add(Expressions.declare(0, valueVariable, null));
    final List<RexNode> operandList = call.getOperands();
    implementRecursively(this, operandList, valueVariable, 0);
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("case_when_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    final Result result = new Result(isNullVariable, valueVariable);
    rexResultMap.put(call, result);
    return result;
  }

  /**
   * Case statements of the form:
   * {@code CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END}.
   * When {@code a = true}, returns {@code b};
   * when {@code c = true}, returns {@code d};
   * else returns {@code e}.
   *
   * <p>We generate code that looks like:
   *
   * <blockquote><pre>{@code
   *      int case_when_value;
   *      ......code for a......
   *      if (!a_isNull && a_value) {
   *          ......code for b......
   *          case_when_value = res(b_isNull, b_value);
   *      } else {
   *          ......code for c......
   *          if (!c_isNull && c_value) {
   *              ......code for d......
   *              case_when_value = res(d_isNull, d_value);
   *          } else {
   *              ......code for e......
   *              case_when_value = res(e_isNull, e_value);
   *          }
   *      }
   * }</pre></blockquote>
   */
  private static void implementRecursively(final RexToLixTranslator currentTranslator,
      final List<RexNode> operandList, final ParameterExpression valueVariable, int pos) {
    final BlockBuilder currentBlockBuilder = currentTranslator.getBlockBuilder();
    final List<@Nullable Type> storageTypes = EnumUtils.internalTypes(operandList);
    // [ELSE] clause
    if (pos == operandList.size() - 1) {
      Expression res = implementCallOperand2(operandList.get(pos),
          storageTypes.get(pos), currentTranslator);
      currentBlockBuilder.add(
          Expressions.statement(
              Expressions.assign(valueVariable,
                  EnumUtils.convert(res, valueVariable.getType()))));
      return;
    }
    // Condition code: !a_isNull && a_value
    final RexNode testerNode = operandList.get(pos);
    final Result testerResult = implementCallOperand(testerNode,
        storageTypes.get(pos), currentTranslator);
    final Expression tester = Expressions.andAlso(
        Expressions.not(testerResult.isNullVariable),
        testerResult.valueVariable);
    // Code for {if} branch
    final RexNode ifTrueNode = operandList.get(pos + 1);
    final BlockBuilder ifTrueBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifTrueTranslator =
        currentTranslator.setBlock(ifTrueBlockBuilder);
    final Expression ifTrueRes = implementCallOperand2(ifTrueNode,
        storageTypes.get(pos + 1), ifTrueTranslator);
    // Assign the value: case_when_value = ifTrueRes
    ifTrueBlockBuilder.add(
        Expressions.statement(
            Expressions.assign(valueVariable,
                EnumUtils.convert(ifTrueRes, valueVariable.getType()))));
    final BlockStatement ifTrue = ifTrueBlockBuilder.toBlock();
    // There is no [ELSE] clause
    if (pos + 1 == operandList.size() - 1) {
      currentBlockBuilder.add(
          Expressions.ifThen(tester, ifTrue));
      return;
    }
    // Generate code for {else} branch recursively
    final BlockBuilder ifFalseBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifFalseTranslator =
        currentTranslator.setBlock(ifFalseBlockBuilder);
    implementRecursively(ifFalseTranslator, operandList, valueVariable, pos + 2);
    final BlockStatement ifFalse = ifFalseBlockBuilder.toBlock();
    currentBlockBuilder.add(
        Expressions.ifThenElse(tester, ifTrue, ifFalse));
  }

  private Result toInnerStorageType(final Result result, final Type storageType) {
    final Expression valueExpression =
        EnumUtils.toInternal(result.valueVariable, storageType);
    if (valueExpression.equals(result.valueVariable)) {
      return result;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = result.isNullVariable;
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
    final Pair<RexNode, @Nullable Type> key = Pair.of(dynamicParam, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final Type storageType = currentStorageType != null
        ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());
    final Expression valueExpression = EnumUtils.convert(
        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("?" + dynamicParam.getIndex())),
        storageType);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(), list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valueVariable)));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return result;
  }

  @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
    final Pair<RexNode, @Nullable Type> key = Pair.of(fieldAccess, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final RexNode target = deref(fieldAccess.getReferenceExpr());
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
    case CORREL_VARIABLE:
      if (correlates == null) {
        throw new RuntimeException("Cannot translate " + fieldAccess
            + " since correlate variables resolver is not defined");
      }
      final RexToLixTranslator.InputGetter getter =
          correlates.apply(((RexCorrelVariable) target).getName());
      final Expression input = getter.field(
          list, fieldIndex, currentStorageType);
      final Expression condition = checkNull(input);
      final ParameterExpression valueVariable =
          Expressions.parameter(input.getType(), list.newName("corInp_value"));
      list.add(Expressions.declare(Modifier.FINAL, valueVariable, input));
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
      final Expression isNullExpression = Expressions.condition(
          condition,
          RexImpTable.TRUE_EXPR,
          checkNull(valueVariable));
      list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      final Result result1 = new Result(isNullVariable, valueVariable);
      rexWithStorageTypeResultMap.put(key, result1);
      return result1;
    default:
      RexNode rxIndex =
          builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
      RexNode rxName =
          builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
      RexCall accessCall = (RexCall) builder.makeCall(
          fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
          ImmutableList.of(target, rxIndex, rxName));
      final Result result2 = accessCall.accept(this);
      rexWithStorageTypeResultMap.put(key, result2);
      return result2;
    }
  }

  @Override public Result visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable
        + ". Correlated variables should always be referenced by field access");
  }

  @Override public Result visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public Result visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return visitInputRef(fieldRef);
  }

  Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.FALSE_EXPR;
    }
    return Expressions.equal(expr, RexImpTable.NULL_EXPR);
  }

  Expression checkNotNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.TRUE_EXPR;
    }
    return Expressions.notEqual(expr, RexImpTable.NULL_EXPR);
  }

  BlockBuilder getBlockBuilder() {
    return list;
  }

  Expression getLiteral(Expression literalVariable) {
    return requireNonNull(literalMap.get(literalVariable),
        () -> "literalMap.get(literalVariable) for " + literalVariable);
  }

  /** Returns the value of a literal. */
  @Nullable Object getLiteralValue(@Nullable Expression expr) {
    if (expr instanceof ParameterExpression) {
      final Expression constantExpr = literalMap.get(expr);
      return getLiteralValue(constantExpr);
    }
    if (expr instanceof ConstantExpression) {
      return ((ConstantExpression) expr).value;
    }
    return null;
  }

  List<Result> getCallOperandResult(RexCall call) {
    return requireNonNull(callOperandResultMap.get(call),
        () -> "callOperandResultMap.get(call) for " + call);
  }

  /** Returns an expression that yields the function object whose method
   * we are about to call.
   *
   * <p>It might be 'new MyFunction()', but it also might be a reference
   * to a static field 'F', defined by
   * 'static final MyFunction F = new MyFunction()'.
   *
   * <p>If there is a constructor that takes a {@link FunctionContext}
   * argument, we call that, passing in the values of arguments that are
   * literals; this allows the function to do some computation at load time.
   *
   * <p>If the call is "f(1, 2 + 3, 'foo')" and "f" is implemented by method
   * "eval(int, int, String)" in "class MyFun", the expression might be
   * "new MyFunction(FunctionContexts.of(new Object[] {1, null, "foo"})".
   *
   * @param method Method that implements the UDF
   * @param call Call to the UDF
   * @return New expression
   */
  Expression functionInstance(RexCall call, Method method) {
    final RexCallBinding callBinding =
        RexCallBinding.create(typeFactory, call, program, ImmutableList.of());
    final Expression target = getInstantiationExpression(method, callBinding);
    return appendConstant("f", target);
  }

  /** Helper for {@link #functionInstance}. */
  private Expression getInstantiationExpression(Method method,
      RexCallBinding callBinding) {
    final Class<?> declaringClass = method.getDeclaringClass();
    // If the UDF class has a constructor that takes a Context argument,
    // use that.
    try {
      final Constructor<?> constructor =
          declaringClass.getConstructor(FunctionContext.class);
      final List<Expression> constantArgs = new ArrayList<>();
      //noinspection unchecked
      Ord.forEach(method.getParameterTypes(),
          (parameterType, i) ->
              constantArgs.add(
                  callBinding.isOperandLiteral(i, true)
                      ? appendConstant("_arg",
                      Expressions.constant(
                          callBinding.getOperandLiteralValue(i,
                              Primitive.box(parameterType))))
                      : Expressions.constant(null)));
      final Expression context =
          Expressions.call(BuiltInMethod.FUNCTION_CONTEXTS_OF.method,
              DataContext.ROOT,
              Expressions.newArrayInit(Object.class, constantArgs));
      return Expressions.new_(constructor, context);
    } catch (NoSuchMethodException e) {
      // ignore
    }
    // The UDF class must have a public zero-args constructor.
    // Assume that the validator checked already.
    return Expressions.new_(declaringClass);
  }

  /** Stores a constant expression in a variable. */
  private Expression appendConstant(String name, Expression e) {
    if (staticList != null) {
      // If name is "camelCase", upperName is "CAMEL_CASE".
      final String upperName =
          CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name);
      return staticList.append(upperName, e);
    } else {
      return list.append(name, e);
    }
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, @Nullable Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private final ImmutableMap<Expression, PhysType> inputs;

    @Deprecated // to be removed before 2.0
    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this(mapOf(inputs));
    }

    public InputGetterImpl(Expression e, PhysType physType) {
      this(ImmutableMap.of(e, physType));
    }

    public InputGetterImpl(Map<Expression, PhysType> inputs) {
      this.inputs = ImmutableMap.copyOf(inputs);
    }

    private static <K, V> Map<K, V> mapOf(
        Iterable<? extends Map.Entry<K, V>> entries) {
      ImmutableMap.Builder<K, V> b = ImmutableMap.builder();
      Pair.forEach(entries, b::put);
      return b.build();
    }

    @Override public Expression field(BlockBuilder list, int index, @Nullable Type storageType) {
      int offset = 0;
      for (Map.Entry<Expression, PhysType> input : inputs.entrySet()) {
        final PhysType physType = input.getValue();
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left = list.append("current", input.getKey());
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Result of translating a {@code RexNode}. */
  public static class Result {
    final ParameterExpression isNullVariable;
    final ParameterExpression valueVariable;

    public Result(ParameterExpression isNullVariable,
        ParameterExpression valueVariable) {
      this.isNullVariable = isNullVariable;
      this.valueVariable = valueVariable;
    }
  }
}
