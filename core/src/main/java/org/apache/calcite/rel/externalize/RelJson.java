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
package org.apache.calcite.rel.externalize;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.rel.RelDistributions.EMPTY;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Utilities for converting {@link org.apache.calcite.rel.RelNode}
 * into JSON format.
 */
public class RelJson {
  private final Map<String, Constructor> constructorMap = new HashMap<>();
  private final @Nullable JsonBuilder jsonBuilder;
  private final InputTranslator inputTranslator;

  public static final List<String> PACKAGES =
      ImmutableList.of(
          "org.apache.calcite.rel.",
          "org.apache.calcite.rel.core.",
          "org.apache.calcite.rel.logical.",
          "org.apache.calcite.adapter.jdbc.",
          "org.apache.calcite.adapter.jdbc.JdbcRules$");

  /** Private constructor. */
  private RelJson(@Nullable JsonBuilder jsonBuilder,
      InputTranslator inputTranslator) {
    this.jsonBuilder = jsonBuilder;
    this.inputTranslator = requireNonNull(inputTranslator, "inputTranslator");
  }

  /** Creates a RelJson. */
  public RelJson(@Nullable JsonBuilder jsonBuilder) {
    this(jsonBuilder, RelJson::translateInput);
  }

  /** Returns a RelJson with a given InputTranslator. */
  public RelJson withInputTranslator(InputTranslator inputTranslator) {
    if (inputTranslator == this.inputTranslator) {
      return this;
    }
    return new RelJson(jsonBuilder, inputTranslator);
  }

  private JsonBuilder jsonBuilder() {
    return requireNonNull(jsonBuilder, "jsonBuilder");
  }

  @SuppressWarnings("unchecked")
  private static <T extends Object> T get(Map<String, ? extends @Nullable Object> map,
      String key) {
    return (T) requireNonNull(map.get(key), () -> "entry for key " + key);
  }

  private static <T extends Enum<T>> T enumVal(Class<T> clazz, Map<String, Object> map,
      String key) {
    String textValue = get(map, key);
    return requireNonNull(
        Util.enumVal(clazz, textValue),
        () -> "unable to find enum value " + textValue + " in class " + clazz);
  }

  public RelNode create(Map<String, Object> map) {
    String type = get(map, "type");
    Constructor constructor = getConstructor(type);
    try {
      return (RelNode) constructor.newInstance(map);
    } catch (InstantiationException | ClassCastException | InvocationTargetException
        | IllegalAccessException e) {
      throw new RuntimeException(
          "while invoking constructor for type '" + type + "'", e);
    }
  }

  public Constructor getConstructor(String type) {
    Constructor constructor = constructorMap.get(type);
    if (constructor == null) {
      Class clazz = typeNameToClass(type);
      try {
        //noinspection unchecked
        constructor = clazz.getConstructor(RelInput.class);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("class does not have required constructor, "
            + clazz + "(RelInput)");
      }
      constructorMap.put(type, constructor);
    }
    return constructor;
  }

  /**
   * Converts a type name to a class. E.g. {@code getClass("LogicalProject")}
   * returns {@link org.apache.calcite.rel.logical.LogicalProject}.class.
   */
  public Class typeNameToClass(String type) {
    if (!type.contains(".")) {
      for (String package_ : PACKAGES) {
        try {
          return Class.forName(package_ + type);
        } catch (ClassNotFoundException e) {
          // ignore
        }
      }
    }
    try {
      return Class.forName(type);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("unknown type " + type);
    }
  }

  /**
   * Inverse of {@link #typeNameToClass}.
   */
  public String classToTypeName(Class<? extends RelNode> class_) {
    final String canonicalName = class_.getName();
    for (String package_ : PACKAGES) {
      if (canonicalName.startsWith(package_)) {
        String remaining = canonicalName.substring(package_.length());
        if (remaining.indexOf('.') < 0 && remaining.indexOf('$') < 0) {
          return remaining;
        }
      }
    }
    return canonicalName;
  }

  /** Default implementation of
   * {@link InputTranslator#translateInput(RelJson, int, Map, RelInput)}. */
  private static RexNode translateInput(RelJson relJson, int input,
      Map<String, @Nullable Object> map, RelInput relInput) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    // Check if it is a local ref.
    if (map.containsKey("type")) {
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      final RelDataType type = relJson.toType(typeFactory, get(map, "type"));
      return rexBuilder.makeLocalRef(type, input);
    }
    int i = input;
    final List<RelNode> relNodes = relInput.getInputs();
    for (RelNode inputNode : relNodes) {
      final RelDataType rowType = inputNode.getRowType();
      if (i < rowType.getFieldCount()) {
        final RelDataTypeField field = rowType.getFieldList().get(i);
        return rexBuilder.makeInputRef(field.getType(), input);
      }
      i -= rowType.getFieldCount();
    }
    throw new RuntimeException("input field " + input + " is out of range");
  }

  public Object toJson(RelCollationImpl node) {
    final List<Object> list = new ArrayList<>();
    for (RelFieldCollation fieldCollation : node.getFieldCollations()) {
      final Map<String, @Nullable Object> map = jsonBuilder().map();
      map.put("field", fieldCollation.getFieldIndex());
      map.put("direction", fieldCollation.getDirection().name());
      map.put("nulls", fieldCollation.nullDirection.name());
      list.add(map);
    }
    return list;
  }

  public RelCollation toCollation(
      List<Map<String, Object>> jsonFieldCollations) {
    final List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (Map<String, Object> map : jsonFieldCollations) {
      fieldCollations.add(toFieldCollation(map));
    }
    return RelCollations.of(fieldCollations);
  }

  public RelFieldCollation toFieldCollation(Map<String, Object> map) {
    final Integer field = get(map, "field");
    final RelFieldCollation.Direction direction =
        enumVal(RelFieldCollation.Direction.class,
            map, "direction");
    final RelFieldCollation.NullDirection nullDirection =
        enumVal(RelFieldCollation.NullDirection.class,
            map, "nulls");
    return new RelFieldCollation(field, direction, nullDirection);
  }

  public RelDistribution toDistribution(Map<String, Object> map) {
    final RelDistribution.Type type =
        enumVal(RelDistribution.Type.class,
            map, "type");

    ImmutableIntList list = EMPTY;
    List<Integer> keys = (List<Integer>) map.get("keys");
    if (keys != null) {
      list = ImmutableIntList.copyOf(keys);
    }
    return RelDistributions.of(type, list);
  }

  private Object toJson(RelDistribution relDistribution) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    map.put("type", relDistribution.getType().name());
    if (!relDistribution.getKeys().isEmpty()) {
      map.put("keys", relDistribution.getKeys());
    }
    return map;
  }

  public RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
    if (o instanceof List) {
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> jsonList = (List<Map<String, Object>>) o;
      final RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (Map<String, Object> jsonMap : jsonList) {
        builder.add(get(jsonMap, "name"), toType(typeFactory, jsonMap));
      }
      return builder.build();
    } else if (o instanceof Map) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> map = (Map<String, Object>) o;
      final RelDataType type = getRelDataType(typeFactory, map);
      final boolean nullable = get(map, "nullable");
      return typeFactory.createTypeWithNullability(type, nullable);
    } else {
      final SqlTypeName sqlTypeName = requireNonNull(
          Util.enumVal(SqlTypeName.class, (String) o),
          () -> "unable to find enum value " + o + " in class " + SqlTypeName.class);
      return typeFactory.createSqlType(sqlTypeName);
    }
  }

  private RelDataType getRelDataType(RelDataTypeFactory typeFactory, Map<String, Object> map) {
    final Object fields = map.get("fields");
    if (fields != null) {
      // Nested struct
      return toType(typeFactory, fields);
    }
    final SqlTypeName sqlTypeName =
        enumVal(SqlTypeName.class, map, "type");
    final Object component;
    final RelDataType componentType;
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
      TimeUnit startUnit = sqlTypeName.getStartUnit();
      TimeUnit endUnit = sqlTypeName.getEndUnit();
      return typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO));

    case ARRAY:
      component = requireNonNull(map.get("component"), "component");
      componentType = toType(typeFactory, component);
      return typeFactory.createArrayType(componentType, -1);

    case MAP:
      Object key = get(map, "key");
      Object value = get(map, "value");
      RelDataType keyType = toType(typeFactory, key);
      RelDataType valueType = toType(typeFactory, value);
      return typeFactory.createMapType(keyType, valueType);

    case MULTISET:
      component = requireNonNull(map.get("component"), "component");
      componentType = toType(typeFactory, component);
      return typeFactory.createMultisetType(componentType, -1);

    default:
      final Integer precision = (Integer) map.get("precision");
      final Integer scale = (Integer) map.get("scale");
      if (precision == null) {
        return typeFactory.createSqlType(sqlTypeName);
      } else if (scale == null) {
        return typeFactory.createSqlType(sqlTypeName, precision);
      } else {
        return typeFactory.createSqlType(sqlTypeName, precision, scale);
      }
    }
  }

  public Object toJson(AggregateCall node) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    final Map<String, @Nullable Object> aggMap = toJson(node.getAggregation());
    if (node.getAggregation().getFunctionType().isUserDefined()) {
      aggMap.put("class", node.getAggregation().getClass().getName());
    }
    map.put("agg", aggMap);
    map.put("type", toJson(node.getType()));
    map.put("distinct", node.isDistinct());
    map.put("operands", node.getArgList());
    map.put("name", node.getName());
    return map;
  }

  public @Nullable Object toJson(@Nullable Object value) {
    if (value == null
        || value instanceof Number
        || value instanceof String
        || value instanceof Boolean) {
      return value;
    } else if (value instanceof RexNode) {
      return toJson((RexNode) value);
    } else if (value instanceof RexWindow) {
      return toJson((RexWindow) value);
    } else if (value instanceof RexFieldCollation) {
      return toJson((RexFieldCollation) value);
    } else if (value instanceof RexWindowBound) {
      return toJson((RexWindowBound) value);
    } else if (value instanceof CorrelationId) {
      return toJson((CorrelationId) value);
    } else if (value instanceof List || value instanceof Set) {
      final List<@Nullable Object> list = jsonBuilder().list();
      for (Object o : (Collection<?>) value) {
        list.add(toJson(o));
      }
      return list;
    } else if (value instanceof ImmutableBitSet) {
      final List<@Nullable Object> list = jsonBuilder().list();
      for (Integer integer : (ImmutableBitSet) value) {
        list.add(toJson(integer));
      }
      return list;
    } else if (value instanceof AggregateCall) {
      return toJson((AggregateCall) value);
    } else if (value instanceof RelCollationImpl) {
      return toJson((RelCollationImpl) value);
    } else if (value instanceof RelDataType) {
      return toJson((RelDataType) value);
    } else if (value instanceof RelDataTypeField) {
      return toJson((RelDataTypeField) value);
    } else if (value instanceof RelDistribution) {
      return toJson((RelDistribution) value);
    } else {
      throw new UnsupportedOperationException("type not serializable: "
          + value + " (type " + value.getClass().getCanonicalName() + ")");
    }
  }

  private Object toJson(RelDataType node) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (node.isStruct()) {
      final List<@Nullable Object> list = jsonBuilder().list();
      for (RelDataTypeField field : node.getFieldList()) {
        list.add(toJson(field));
      }
      map.put("fields", list);
      map.put("nullable", node.isNullable());
    } else {
      map.put("type", node.getSqlTypeName().name());
      map.put("nullable", node.isNullable());
      if (node.getComponentType() != null) {
        map.put("component", toJson(node.getComponentType()));
      }
      RelDataType keyType = node.getKeyType();
      if (keyType != null) {
        map.put("key", toJson(keyType));
      }
      RelDataType valueType = node.getValueType();
      if (valueType != null) {
        map.put("value", toJson(valueType));
      }
      if (node.getSqlTypeName().allowsPrec()) {
        map.put("precision", node.getPrecision());
      }
      if (node.getSqlTypeName().allowsScale()) {
        map.put("scale", node.getScale());
      }
    }
    return map;
  }

  private Object toJson(RelDataTypeField node) {
    final Map<String, @Nullable Object> map;
    if (node.getType().isStruct()) {
      map = jsonBuilder().map();
      map.put("fields", toJson(node.getType()));
      map.put("nullable", node.getType().isNullable());
    } else {
      //noinspection unchecked
      map = (Map<String, @Nullable Object>) toJson(node.getType());
    }
    map.put("name", node.getName());
    return map;
  }

  private static Object toJson(CorrelationId node) {
    return node.getId();
  }

  private Object toJson(RexNode node) {
    final Map<String, @Nullable Object> map;
    switch (node.getKind()) {
    case FIELD_ACCESS:
      map = jsonBuilder().map();
      final RexFieldAccess fieldAccess = (RexFieldAccess) node;
      map.put("field", fieldAccess.getField().getName());
      map.put("expr", toJson(fieldAccess.getReferenceExpr()));
      return map;
    case LITERAL:
      final RexLiteral literal = (RexLiteral) node;
      final Object value = literal.getValue3();
      map = jsonBuilder().map();
      map.put("literal", RelEnumTypes.fromEnum(value));
      map.put("type", toJson(node.getType()));
      return map;
    case INPUT_REF:
      map = jsonBuilder().map();
      map.put("input", ((RexSlot) node).getIndex());
      map.put("name", ((RexSlot) node).getName());
      return map;
    case LOCAL_REF:
      map = jsonBuilder().map();
      map.put("input", ((RexSlot) node).getIndex());
      map.put("name", ((RexSlot) node).getName());
      map.put("type", toJson(node.getType()));
      return map;
    case CORREL_VARIABLE:
      map = jsonBuilder().map();
      map.put("correl", ((RexCorrelVariable) node).getName());
      map.put("type", toJson(node.getType()));
      return map;
    default:
      if (node instanceof RexCall) {
        final RexCall call = (RexCall) node;
        map = jsonBuilder().map();
        map.put("op", toJson(call.getOperator()));
        final List<@Nullable Object> list = jsonBuilder().list();
        for (RexNode operand : call.getOperands()) {
          list.add(toJson(operand));
        }
        map.put("operands", list);
        switch (node.getKind()) {
        case CAST:
          map.put("type", toJson(node.getType()));
          break;
        default:
          break;
        }
        if (call.getOperator() instanceof SqlFunction) {
          if (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined()) {
            SqlOperator op = call.getOperator();
            map.put("class", op.getClass().getName());
            map.put("type", toJson(node.getType()));
            map.put("deterministic", op.isDeterministic());
            map.put("dynamic", op.isDynamicFunction());
          }
        }
        if (call instanceof RexOver) {
          RexOver over = (RexOver) call;
          map.put("distinct", over.isDistinct());
          map.put("type", toJson(node.getType()));
          map.put("window", toJson(over.getWindow()));
        }
        return map;
      }
      throw new UnsupportedOperationException("unknown rex " + node);
    }
  }

  private Object toJson(RexWindow window) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (window.partitionKeys.size() > 0) {
      map.put("partition", toJson(window.partitionKeys));
    }
    if (window.orderKeys.size() > 0) {
      map.put("order", toJson(window.orderKeys));
    }
    if (window.getLowerBound() == null) {
      // No ROWS or RANGE clause
    } else if (window.getUpperBound() == null) {
      if (window.isRows()) {
        map.put("rows-lower", toJson(window.getLowerBound()));
      } else {
        map.put("range-lower", toJson(window.getLowerBound()));
      }
    } else {
      if (window.isRows()) {
        map.put("rows-lower", toJson(window.getLowerBound()));
        map.put("rows-upper", toJson(window.getUpperBound()));
      } else {
        map.put("range-lower", toJson(window.getLowerBound()));
        map.put("range-upper", toJson(window.getUpperBound()));
      }
    }
    return map;
  }

  private Object toJson(RexFieldCollation collation) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    map.put("expr", toJson(collation.left));
    map.put("direction", collation.getDirection().name());
    map.put("null-direction", collation.getNullDirection().name());
    return map;
  }

  private Object toJson(RexWindowBound windowBound) {
    final Map<String, @Nullable Object> map = jsonBuilder().map();
    if (windowBound.isCurrentRow()) {
      map.put("type", "CURRENT_ROW");
    } else if (windowBound.isUnbounded()) {
      map.put("type", windowBound.isPreceding() ? "UNBOUNDED_PRECEDING" : "UNBOUNDED_FOLLOWING");
    } else {
      map.put("type", windowBound.isPreceding() ? "PRECEDING" : "FOLLOWING");
      RexNode offset = requireNonNull(windowBound.getOffset(),
          () -> "getOffset for window bound " + windowBound);
      map.put("offset", toJson(offset));
    }
    return map;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @PolyNull RexNode toRex(RelInput relInput, @PolyNull Object o) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (o == null) {
      return null;
    } else if (o instanceof Map) {
      final Map<String, @Nullable Object> map = (Map) o;
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      if (map.containsKey("op")) {
        final Map<String, @Nullable Object> opMap = get(map, "op");
        if (map.containsKey("class")) {
          opMap.put("class", get(map, "class"));
        }
        final List operands = get(map, "operands");
        final List<RexNode> rexOperands = toRexList(relInput, operands);
        final Object jsonType = map.get("type");
        final Map window = (Map) map.get("window");
        if (window != null) {
          final SqlAggFunction operator = requireNonNull(toAggregation(opMap), "operator");
          final RelDataType type = toType(typeFactory, requireNonNull(jsonType, "jsonType"));
          List<RexNode> partitionKeys = new ArrayList<>();
          Object partition = window.get("partition");
          if (partition != null) {
            partitionKeys = toRexList(relInput, (List) partition);
          }
          List<RexFieldCollation> orderKeys = new ArrayList<>();
          if (window.containsKey("order")) {
            addRexFieldCollationList(orderKeys, relInput, (List) window.get("order"));
          }
          final RexWindowBound lowerBound;
          final RexWindowBound upperBound;
          final boolean physical;
          if (window.get("rows-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("rows-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("rows-upper"));
            physical = true;
          } else if (window.get("range-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("range-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("range-upper"));
            physical = false;
          } else {
            // No ROWS or RANGE clause
            // Note: lower and upper bounds are non-nullable, so this branch is not reachable
            lowerBound = null;
            upperBound = null;
            physical = false;
          }
          final boolean distinct = get((Map<String, Object>) map, "distinct");
          return rexBuilder.makeOver(type, operator, rexOperands, partitionKeys,
              ImmutableList.copyOf(orderKeys),
              requireNonNull(lowerBound, "lowerBound"),
              requireNonNull(upperBound, "upperBound"),
              physical,
              true, false, distinct, false);
        } else {
          final SqlOperator operator = requireNonNull(toOp(opMap), "operator");
          final RelDataType type;
          if (jsonType != null) {
            type = toType(typeFactory, jsonType);
          } else {
            type = rexBuilder.deriveReturnType(operator, rexOperands);
          }
          return rexBuilder.makeCall(type, operator, rexOperands);
        }
      }
      final Integer input = (Integer) map.get("input");
      if (input != null) {
        return inputTranslator.translateInput(this, input, map, relInput);
      }
      final String field = (String) map.get("field");
      if (field != null) {
        final Object jsonExpr = get(map, "expr");
        final RexNode expr = toRex(relInput, jsonExpr);
        return rexBuilder.makeFieldAccess(expr, field, true);
      }
      final String correl = (String) map.get("correl");
      if (correl != null) {
        final Object jsonType = get(map, "type");
        RelDataType type = toType(typeFactory, jsonType);
        return rexBuilder.makeCorrel(type, new CorrelationId(correl));
      }
      if (map.containsKey("literal")) {
        Object literal = map.get("literal");
        if (literal == null) {
          final RelDataType type = toType(typeFactory, get(map, "type"));
          return rexBuilder.makeNullLiteral(type);
        }
        if (!map.containsKey("type")) {
          // In previous versions, type was not specified for all literals.
          // To keep backwards compatibility, if type is not specified
          // we just interpret the literal
          return toRex(relInput, literal);
        }
        final RelDataType type = toType(typeFactory, get(map, "type"));
        if (type.getSqlTypeName() == SqlTypeName.SYMBOL) {
          literal = RelEnumTypes.toEnum((String) literal);
        }
        return rexBuilder.makeLiteral(literal, type);
      }
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    } else if (o instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) o);
    } else if (o instanceof String) {
      return rexBuilder.makeLiteral((String) o);
    } else if (o instanceof Number) {
      final Number number = (Number) o;
      if (number instanceof Double || number instanceof Float) {
        return rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(number.doubleValue()));
      } else {
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(number.longValue()));
      }
    } else {
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    }
  }

  private void addRexFieldCollationList(
      List<RexFieldCollation> list,
      RelInput relInput, @Nullable List<Map<String, Object>> order) {
    if (order == null) {
      return;
    }

    for (Map<String, Object> o : order) {
      RexNode expr = requireNonNull(toRex(relInput, o.get("expr")), "expr");
      Set<SqlKind> directions = new HashSet<>();
      if (Direction.valueOf(get(o, "direction")) == Direction.DESCENDING) {
        directions.add(SqlKind.DESCENDING);
      }
      if (NullDirection.valueOf(get(o, "null-direction")) == NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      } else {
        directions.add(SqlKind.NULLS_LAST);
      }
      list.add(new RexFieldCollation(expr, directions));
    }
  }

  private @Nullable RexWindowBound toRexWindowBound(RelInput input,
      @Nullable Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    final String type = get(map, "type");
    switch (type) {
    case "CURRENT_ROW":
      return RexWindowBounds.CURRENT_ROW;
    case "UNBOUNDED_PRECEDING":
      return RexWindowBounds.UNBOUNDED_PRECEDING;
    case "UNBOUNDED_FOLLOWING":
      return RexWindowBounds.UNBOUNDED_FOLLOWING;
    case "PRECEDING":
      return RexWindowBounds.preceding(toRex(input, get(map, "offset")));
    case "FOLLOWING":
      return RexWindowBounds.following(toRex(input, get(map, "offset")));
    default:
      throw new UnsupportedOperationException("cannot convert type to rex window bound " + type);
    }
  }

  private List<RexNode> toRexList(RelInput relInput, List operands) {
    final List<RexNode> list = new ArrayList<>();
    for (Object operand : operands) {
      list.add(toRex(relInput, operand));
    }
    return list;
  }

  @Nullable SqlOperator toOp(Map<String, ? extends @Nullable Object> map) {
    // in case different operator has the same kind, check with both name and kind.
    String name = get(map, "name");
    String kind = get(map, "kind");
    String syntax = get(map, "syntax");
    SqlKind sqlKind = SqlKind.valueOf(kind);
    SqlSyntax  sqlSyntax = SqlSyntax.valueOf(syntax);
    List<SqlOperator> operators = new ArrayList<>();
    SqlStdOperatorTable.instance().lookupOperatorOverloads(
        new SqlIdentifier(name, new SqlParserPos(0, 0)),
        null,
        sqlSyntax,
        operators,
        SqlNameMatchers.liberal());
    for (SqlOperator operator: operators) {
      if (operator.kind == sqlKind) {
        return operator;
      }
    }
    String class_ = (String) map.get("class");
    if (class_ != null) {
      return AvaticaUtils.instantiatePlugin(SqlOperator.class, class_);
    }
    throw RESOURCE.noOperator(name, kind, syntax).ex();
  }

  @Nullable SqlAggFunction toAggregation(Map<String, ? extends @Nullable Object> map) {
    return (SqlAggFunction) toOp(map);
  }

  private Map<String, @Nullable Object> toJson(SqlOperator operator) {
    // User-defined operators are not yet handled.
    Map<String, @Nullable Object> map = jsonBuilder().map();
    map.put("name", operator.getName());
    map.put("kind", operator.kind.toString());
    map.put("syntax", operator.getSyntax().toString());
    return map;
  }

  /**
   * Translates a JSON expression into a RexNode,
   * using a given {@link InputTranslator} to transform JSON objects that
   * represent input references into RexNodes.
   *
   * @param cluster The optimization environment
   * @param translator Input translator
   * @param o JSON object
   * @return the transformed RexNode
   */
  public static RexNode readExpression(RelOptCluster cluster,
      InputTranslator translator, Map<String, Object> o) {
    RelInput relInput = new RelInputForCluster(cluster);
    return new RelJson(null, translator).toRex(relInput, o);
  }

  /**
   * Special context from which a relational expression can be initialized,
   * reading from a serialized form of the relational expression.
   *
   * <p>Contains only a cluster and an empty list of inputs;
   * most methods throw {@link UnsupportedOperationException}.
   */
  private static class RelInputForCluster implements RelInput {
    private final RelOptCluster cluster;

    RelInputForCluster(RelOptCluster cluster) {
      this.cluster = cluster;
    }
    @Override public RelOptCluster getCluster() {
      return cluster;
    }

    @Override public RelTraitSet getTraitSet() {
      throw new UnsupportedOperationException();
    }

    @Override public RelOptTable getTable(String table) {
      throw new UnsupportedOperationException();
    }

    @Override public RelNode getInput() {
      throw new UnsupportedOperationException();
    }

    @Override public List<RelNode> getInputs() {
      return ImmutableList.of();
    }

    @Override public @Nullable RexNode getExpression(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public ImmutableBitSet getBitSet(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable List<ImmutableBitSet> getBitSetList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public List<AggregateCall> getAggregateCalls(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable Object get(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable String getString(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public float getFloat(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public <E extends Enum<E>> @Nullable E getEnum(
        String tag, Class<E> enumClass) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable List<RexNode> getExpressionList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable List<String> getStringList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable List<Integer> getIntegerList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public @Nullable List<List<Integer>> getIntegerListList(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public RelDataType getRowType(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
      throw new UnsupportedOperationException();
    }

    @Override public RelCollation getCollation() {
      throw new UnsupportedOperationException();
    }

    @Override public RelDistribution getDistribution() {
      throw new UnsupportedOperationException();
    }

    @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(String tag) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean getBoolean(String tag, boolean default_) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Translates a JSON object that represents an input reference into a RexNode.
   */
  @FunctionalInterface
  public interface InputTranslator {
    /**
     * Transforms an input reference into a RexNode.
     *
     * @param relJson RelJson
     * @param input Ordinal of input field
     * @param map JSON object representing an input reference
     * @param relInput Description of input(s)
     * @return RexNode representing an input reference
     */
    RexNode translateInput(RelJson relJson, int input,
        Map<String, @Nullable Object> map, RelInput relInput);
  }
}
