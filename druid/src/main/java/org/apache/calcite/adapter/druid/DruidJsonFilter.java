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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Filter element of a Druid "groupBy" or "topN" query.
 */
abstract class DruidJsonFilter implements DruidJson {

  private static final SimpleDateFormat DATE_FORMATTER = getDateFormatter();


  /**
   * @param rexNode    rexNode to translate to Druid Json Filter
   * @param rowType    rowType associated to rexNode
   * @param druidQuery druid query
   *
   * @return Druid Json filter or null if it can not translate
   */
  @Nullable
  private static DruidJsonFilter toEqualityKindDruidFilter(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    if (rexNode.getKind() != SqlKind.EQUALS && rexNode.getKind() != SqlKind.NOT_EQUALS) {
      throw new AssertionError(
          DruidQuery.format("Expecting EQUALS or NOT_EQUALS but got [%s]", rexNode.getKind()));
    }
    final RexCall rexCall = (RexCall) rexNode;
    if (rexCall.getOperands().size() < 2) {
      return null;
    }
    final RexLiteral rexLiteral;
    final RexNode refNode;
    final RexNode lhs = rexCall.getOperands().get(0);
    final RexNode rhs = rexCall.getOperands().get(1);
    if (lhs.getKind() == SqlKind.LITERAL && rhs.getKind() != SqlKind.LITERAL) {
      rexLiteral = (RexLiteral) lhs;
      refNode = rhs;
    } else if (rhs.getKind() == SqlKind.LITERAL && lhs.getKind() != SqlKind.LITERAL) {
      rexLiteral = (RexLiteral) rhs;
      refNode = lhs;
    } else {
      // must have at least one literal
      return null;
    }

    if (RexLiteral.isNullLiteral(rexLiteral)) {
      // we are not handling is NULL filter here thus we bail out if Literal is null
      return null;
    }
    final String literalValue = toDruidLiteral(rexLiteral, rowType, druidQuery);
    if (literalValue == null) {
      // can not translate literal better bail out
      return null;
    }
    final boolean isNumeric = refNode.getType().getFamily() == SqlTypeFamily.NUMERIC
        || rexLiteral.getType().getFamily() == SqlTypeFamily.NUMERIC;
    final Pair<String, ExtractionFunction> druidColumn = DruidQuery.toDruidColumn(refNode, rowType,
        druidQuery);
    final String columnName = druidColumn.left;
    final ExtractionFunction extractionFunction = druidColumn.right;
    if (columnName == null) {
      // no column name better bail out.
      return null;
    }
    final DruidJsonFilter partialFilter;
    if (isNumeric) {
      //need bound filter since it one of operands is numeric
      partialFilter = new JsonBound(columnName, literalValue, false, literalValue, false, true,
          extractionFunction);
    } else {
      partialFilter = new JsonSelector(columnName, literalValue, extractionFunction);
    }

    if (rexNode.getKind() == SqlKind.EQUALS) {
      return partialFilter;
    }
    return toNotDruidFilter(partialFilter);
  }


  /**
   * @param rexNode    rexNode to translate
   * @param rowType    row type associated to Filter
   * @param druidQuery druid query
   *
   * @return valid Druid Json Bound Filter or null if it can not translate the rexNode.
   */
  @Nullable
  private static DruidJsonFilter toBoundDruidFilter(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    final RexCall rexCall = (RexCall) rexNode;
    final RexLiteral rexLiteral;
    if (rexCall.getOperands().size() < 2) {
      return null;
    }
    final RexNode refNode;
    final RexNode lhs = rexCall.getOperands().get(0);
    final RexNode rhs = rexCall.getOperands().get(1);
    final boolean lhsIsRef;
    if (lhs.getKind() == SqlKind.LITERAL && rhs.getKind() != SqlKind.LITERAL) {
      rexLiteral = (RexLiteral) lhs;
      refNode = rhs;
      lhsIsRef = false;
    } else if (rhs.getKind() == SqlKind.LITERAL && lhs.getKind() != SqlKind.LITERAL) {
      rexLiteral = (RexLiteral) rhs;
      refNode = lhs;
      lhsIsRef = true;
    } else {
      // must have at least one literal
      return null;
    }

    if (RexLiteral.isNullLiteral(rexLiteral)) {
      // we are not handling is NULL filter here thus we bail out if Literal is null
      return null;
    }
    final String literalValue = DruidJsonFilter.toDruidLiteral(rexLiteral, rowType, druidQuery);
    if (literalValue == null) {
      // can not translate literal better bail out
      return null;
    }
    final boolean isNumeric = refNode.getType().getFamily() == SqlTypeFamily.NUMERIC
        || rexLiteral.getType().getFamily() == SqlTypeFamily.NUMERIC;
    final Pair<String, ExtractionFunction> druidColumn = DruidQuery.toDruidColumn(refNode, rowType,
        druidQuery);
    final String columnName = druidColumn.left;
    final ExtractionFunction extractionFunction = druidColumn.right;
    if (columnName == null) {
      // no column name better bail out.
      return null;
    }
    switch (rexCall.getKind()) {
    case LESS_THAN_OR_EQUAL:
    case LESS_THAN:
      if (lhsIsRef) {
        return new JsonBound(columnName, null, false, literalValue,
            rexCall.getKind() == SqlKind.LESS_THAN, isNumeric,
            extractionFunction);
      } else {
        return new JsonBound(columnName, literalValue, rexCall.getKind() == SqlKind.LESS_THAN, null,
            false, isNumeric,
            extractionFunction);
      }
    case GREATER_THAN_OR_EQUAL:
    case GREATER_THAN:
      if (!lhsIsRef) {
        return new JsonBound(columnName, null, false, literalValue,
            rexCall.getKind() == SqlKind.GREATER_THAN, isNumeric,
            extractionFunction);
      } else {
        return new JsonBound(columnName, literalValue, rexCall.getKind() == SqlKind.GREATER_THAN,
            null,
            false, isNumeric,
            extractionFunction);
      }
    default:
      return null;
    }

  }

  /**
   * @param rexNode    rexNode to translate to Druid literal equivalante
   * @param rowType    rowType associated to rexNode
   * @param druidQuery druid Query
   *
   * @return non null string or null if it can not translate to valid Druid equivalent
   */
  @Nullable
  private static String toDruidLiteral(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    final String val;
    final RexLiteral rhsLiteral = (RexLiteral) rexNode;
    if (SqlTypeName.NUMERIC_TYPES.contains(rhsLiteral.getTypeName())) {
      val = String.valueOf(RexLiteral.value(rhsLiteral));
    } else if (SqlTypeName.CHAR_TYPES.contains(rhsLiteral.getTypeName())) {
      val = String.valueOf(RexLiteral.stringValue(rhsLiteral));
    } else if (SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE == rhsLiteral.getTypeName()
        || SqlTypeName.TIMESTAMP == rhsLiteral.getTypeName()
        || SqlTypeName.DATE == rhsLiteral.getTypeName()) {
      Long millisSinceEpoch = DruidDateTimeUtils.literalValue(rexNode);
      if (millisSinceEpoch == null) {
        throw new AssertionError(
            "Cannot translate Literal" + rexNode + " of type "
                + rhsLiteral.getTypeName() + " to TimestampString");
      }
      val = DATE_FORMATTER.format(millisSinceEpoch);
    } else {
      // Don't know how to filter on this kind of literal.
      val = null;
    }
    return val;
  }

  @Nullable
  private static DruidJsonFilter toIsNullKindDruidFilter(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    if (rexNode.getKind() != SqlKind.IS_NULL && rexNode.getKind() != SqlKind.IS_NOT_NULL) {
      throw new AssertionError(
          DruidQuery.format("Expecting IS_NULL or IS_NOT_NULL but got [%s]", rexNode.getKind()));
    }
    final RexCall rexCall = (RexCall) rexNode;
    final RexNode refNode = rexCall.getOperands().get(0);
    Pair<String, ExtractionFunction> druidColumn = DruidQuery
        .toDruidColumn(refNode, rowType, druidQuery);
    final String columnName = druidColumn.left;
    final ExtractionFunction extractionFunction = druidColumn.right;
    if (columnName == null) {
      return null;
    }
    if (rexNode.getKind() == SqlKind.IS_NOT_NULL) {
      return toNotDruidFilter(new JsonSelector(columnName, null, extractionFunction));
    }
    return new JsonSelector(columnName, null, extractionFunction);
  }

  @Nullable
  private static DruidJsonFilter toInKindDruidFilter(RexNode e, RelDataType rowType,
      DruidQuery druidQuery) {
    if (e.getKind() != SqlKind.IN && e.getKind() != SqlKind.NOT_IN) {
      throw new AssertionError(
          DruidQuery.format("Expecting IN or NOT IN but got [%s]", e.getKind()));
    }
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    for (RexNode rexNode : ((RexCall) e).getOperands()) {
      if (rexNode.getKind() == SqlKind.LITERAL) {
        String value = toDruidLiteral(rexNode, rowType, druidQuery);
        if (value == null) {
          return null;
        }
        listBuilder.add(value);
      }
    }
    Pair<String, ExtractionFunction> druidColumn = DruidQuery
        .toDruidColumn(((RexCall) e).getOperands().get(0),
        rowType, druidQuery);
    final String columnName = druidColumn.left;
    final ExtractionFunction extractionFunction = druidColumn.right;
    if (columnName == null) {
      return null;
    }
    if (e.getKind() != SqlKind.NOT_IN) {
      return new DruidJsonFilter.JsonInFilter(columnName, listBuilder.build(), extractionFunction);
    } else {
      return toNotDruidFilter(
          new DruidJsonFilter.JsonInFilter(columnName, listBuilder.build(), extractionFunction));
    }
  }

  @Nullable
  protected static DruidJsonFilter toNotDruidFilter(DruidJsonFilter druidJsonFilter) {
    if (druidJsonFilter == null) {
      return null;
    }
    return new JsonCompositeFilter(Type.NOT, druidJsonFilter);
  }

  @Nullable
  private static DruidJsonFilter toBetweenDruidFilter(RexNode rexNode, RelDataType rowType,
      DruidQuery query) {
    if (rexNode.getKind() != SqlKind.BETWEEN) {
      return null;
    }
    final RexCall rexCall = (RexCall) rexNode;
    if (rexCall.getOperands().size() < 4) {
      return null;
    }
    // BETWEEN (ASYMMETRIC, REF, 'lower-bound', 'upper-bound')
    final RexNode refNode = rexCall.getOperands().get(1);
    final RexNode lhs = rexCall.getOperands().get(2);
    final RexNode rhs = rexCall.getOperands().get(3);

    final String lhsLiteralValue = toDruidLiteral(lhs, rowType, query);
    final String rhsLiteralValue = toDruidLiteral(rhs, rowType, query);
    if (lhsLiteralValue == null || rhsLiteralValue == null) {
      return null;
    }
    final boolean isNumeric = lhs.getType().getFamily() == SqlTypeFamily.NUMERIC
        || lhs.getType().getFamily() == SqlTypeFamily.NUMERIC;
    final Pair<String, ExtractionFunction> druidColumn = DruidQuery
        .toDruidColumn(refNode, rowType, query);
    final String columnName = druidColumn.left;
    final ExtractionFunction extractionFunction = druidColumn.right;

    if (columnName == null) {
      return null;
    }
    return new JsonBound(columnName, lhsLiteralValue, false, rhsLiteralValue,
        false, isNumeric,
        extractionFunction);

  }

  @Nullable
  private static DruidJsonFilter toSimpleDruidFilter(RexNode e, RelDataType rowType,
      DruidQuery druidQuery) {
    switch (e.getKind()) {
    case EQUALS:
    case NOT_EQUALS:
      return toEqualityKindDruidFilter(e, rowType, druidQuery);
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      return toBoundDruidFilter(e, rowType, druidQuery);
    case BETWEEN:
      return toBetweenDruidFilter(e, rowType, druidQuery);
    case IN:
    case NOT_IN:
      return toInKindDruidFilter(e, rowType, druidQuery);
    case IS_NULL:
    case IS_NOT_NULL:
      return toIsNullKindDruidFilter(e, rowType, druidQuery);
    default:
      return null;
    }
  }

  /**
   * @param rexNode    rexNode to translate to Druid Filter
   * @param rowType    rowType of filter input
   * @param druidQuery Druid query
   *
   * @return Druid Json Filters or null when can not translate to valid Druid Filters.
   */
  @Nullable
  static DruidJsonFilter toDruidFilters(final RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    if (rexNode.isAlwaysTrue()) {
      return JsonExpressionFilter.alwaysTrue();
    }
    if (rexNode.isAlwaysFalse()) {
      return JsonExpressionFilter.alwaysFalse();
    }
    switch (rexNode.getKind()) {
    case IS_TRUE:
    case IS_NOT_FALSE:
      return toDruidFilters(Iterables.getOnlyElement(((RexCall) rexNode).getOperands()), rowType,
          druidQuery);
    case IS_NOT_TRUE:
    case IS_FALSE:
      final DruidJsonFilter simpleFilter = toDruidFilters(Iterables
          .getOnlyElement(((RexCall) rexNode).getOperands()), rowType, druidQuery);
      return simpleFilter != null ? new JsonCompositeFilter(Type.NOT, simpleFilter)
          : simpleFilter;
    case AND:
    case OR:
    case NOT:
      final RexCall call = (RexCall) rexNode;
      final List<DruidJsonFilter> jsonFilters = new ArrayList<>();
      for (final RexNode e : call.getOperands()) {
        final DruidJsonFilter druidFilter = toDruidFilters(e, rowType, druidQuery);
        if (druidFilter == null) {
          return null;
        }
        jsonFilters.add(druidFilter);
      }
      return new JsonCompositeFilter(Type.valueOf(rexNode.getKind().name()),
          jsonFilters);
    }

    final DruidJsonFilter simpleLeafFilter = toSimpleDruidFilter(rexNode, rowType, druidQuery);
    return simpleLeafFilter == null
        ? toDruidExpressionFilter(rexNode, rowType, druidQuery)
        : simpleLeafFilter;
  }

  @Nullable
  private static DruidJsonFilter toDruidExpressionFilter(RexNode rexNode, RelDataType rowType,
      DruidQuery query) {
    final String expression = DruidExpressions.toDruidExpression(rexNode, rowType, query);
    return expression == null ? null : new JsonExpressionFilter(expression);
  }

  /**
   * Supported filter types
   */
  protected enum Type {
    AND,
    OR,
    NOT,
    SELECTOR,
    IN,
    BOUND,
    EXPRESSION;

    public String lowercase() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  protected final Type type;

  private DruidJsonFilter(Type type) {
    this.type = type;
  }

  /**
   * Druid Expression filter.
   */
  public static class JsonExpressionFilter extends DruidJsonFilter {
    private final String expression;

    JsonExpressionFilter(String expression) {
      super(Type.EXPRESSION);
      this.expression = Objects.requireNonNull(expression);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("expression", expression);
      generator.writeEndObject();
    }

    /**
     * We need to push to Druid an expression that always evaluates to true.
     */
    private static JsonExpressionFilter alwaysTrue() {
      return new JsonExpressionFilter("1 == 1");
    }

    /**
     * We need to push to Druid an expression that always evaluates to false.
     */
    private static JsonExpressionFilter alwaysFalse() {
      return new JsonExpressionFilter("1 == 2");
    }
  }

  /**
   * Equality filter.
   */
  private static class JsonSelector extends DruidJsonFilter {
    private final String dimension;

    private final String value;

    private final ExtractionFunction extractionFunction;

    private JsonSelector(String dimension, String value,
        ExtractionFunction extractionFunction) {
      super(Type.SELECTOR);
      this.dimension = dimension;
      this.value = value;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      generator.writeStringField("value", value);
      DruidQuery.writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  /**
   * Bound filter.
   */
  @VisibleForTesting
  protected static class JsonBound extends DruidJsonFilter {
    private final String dimension;

    private final String lower;

    private final boolean lowerStrict;

    private final String upper;

    private final boolean upperStrict;

    private final boolean alphaNumeric;

    private final ExtractionFunction extractionFunction;

    protected JsonBound(String dimension, String lower,
        boolean lowerStrict, String upper, boolean upperStrict,
        boolean alphaNumeric, ExtractionFunction extractionFunction) {
      super(Type.BOUND);
      this.dimension = dimension;
      this.lower = lower;
      this.lowerStrict = lowerStrict;
      this.upper = upper;
      this.upperStrict = upperStrict;
      this.alphaNumeric = alphaNumeric;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      if (lower != null) {
        generator.writeStringField("lower", lower);
        generator.writeBooleanField("lowerStrict", lowerStrict);
      }
      if (upper != null) {
        generator.writeStringField("upper", upper);
        generator.writeBooleanField("upperStrict", upperStrict);
      }
      if (alphaNumeric) {
        generator.writeStringField("ordering", "numeric");
      } else {
        generator.writeStringField("ordering", "lexicographic");
      }
      DruidQuery.writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  /**
   * Filter that combines other filters using a boolean operator.
   */
  private static class JsonCompositeFilter extends DruidJsonFilter {
    private final List<? extends DruidJsonFilter> fields;

    private JsonCompositeFilter(Type type,
        Iterable<? extends DruidJsonFilter> fields) {
      super(type);
      this.fields = ImmutableList.copyOf(fields);
    }

    private JsonCompositeFilter(Type type, DruidJsonFilter... fields) {
      this(type, ImmutableList.copyOf(fields));
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      switch (type) {
      case NOT:
        DruidQuery.writeField(generator, "field", fields.get(0));
        break;
      default:
        DruidQuery.writeField(generator, "fields", fields);
      }
      generator.writeEndObject();
    }
  }

  /**
   * IN filter.
   */
  protected static class JsonInFilter extends DruidJsonFilter {
    private final String dimension;

    private final List<String> values;

    private final ExtractionFunction extractionFunction;

    protected JsonInFilter(String dimension, List<String> values,
        ExtractionFunction extractionFunction) {
      super(Type.IN);
      this.dimension = dimension;
      this.values = values;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      DruidQuery.writeField(generator, "values", values);
      DruidQuery.writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  public static DruidJsonFilter getSelectorFilter(String column, String value,
      ExtractionFunction extractionFunction) {
    Objects.requireNonNull(column);
    return new JsonSelector(column, value, extractionFunction);
  }

  /**
   * Druid Having Filter spec
   */
  protected static class JsonDimHavingFilter implements DruidJson {

    private final DruidJsonFilter filter;

    public JsonDimHavingFilter(DruidJsonFilter filter) {
      this.filter = filter;
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", "filter");
      DruidQuery.writeField(generator, "filter", filter);
      generator.writeEndObject();
    }
  }

  private static SimpleDateFormat getDateFormatter() {
    final SimpleDateFormat dateFormatter = new SimpleDateFormat(
        TimeExtractionFunction.ISO_TIME_FORMAT,
        Locale.ROOT);
    dateFormatter.setTimeZone(DateTimeUtils.UTC_ZONE);
    return dateFormatter;
  }
}

// End DruidJsonFilter.java
