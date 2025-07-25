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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.NlsString;

import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Planner rule to push filters and projections to Splunk.
 */
@Value.Enclosing
public class SplunkPushDownRule
    extends RelRule<SplunkPushDownRule.Config> {
  private static final Logger LOGGER =
      StringUtils.getClassTracer(SplunkPushDownRule.class);

  private static final Set<SqlKind> SUPPORTED_OPS =
      ImmutableSet.of(
          SqlKind.CAST,
          SqlKind.EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.NOT_EQUALS,
          SqlKind.LIKE,
          SqlKind.AND,
          SqlKind.OR,
          SqlKind.NOT,
          SqlKind.IS_NULL,
          SqlKind.IS_NOT_NULL,
          SqlKind.IN);

  public static final SplunkPushDownRule PROJECT_ON_FILTER =
      ImmutableSplunkPushDownRule.Config.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalProject.class).oneInput(b1 ->
                  b1.operand(LogicalFilter.class).oneInput(b2 ->
                      b2.operand(LogicalProject.class).oneInput(b3 ->
                          b3.operand(SplunkTableScan.class).noInputs()))))
          .build()
          .withId("proj on filter on proj")
          .toRule();

  public static final SplunkPushDownRule FILTER_ON_PROJECT =
      ImmutableSplunkPushDownRule.Config.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class).oneInput(b1 ->
                  b1.operand(LogicalProject.class).oneInput(b2 ->
                      b2.operand(SplunkTableScan.class).noInputs())))
          .build()
          .withId("filter on proj")
          .toRule();

  public static final SplunkPushDownRule FILTER =
      ImmutableSplunkPushDownRule.Config.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalFilter.class).oneInput(b1 ->
                  b1.operand(SplunkTableScan.class).noInputs()))
          .build()
          .withId("filter")
          .toRule();

  public static final SplunkPushDownRule PROJECT =
      ImmutableSplunkPushDownRule.Config.builder()
          .withOperandSupplier(b0 ->
              b0.operand(LogicalProject.class).oneInput(b1 ->
                  b1.operand(SplunkTableScan.class).noInputs()))
          .build()
          .withId("proj")
          .toRule();

  /**
   * Creates a SplunkPushDownRule.
   */
  protected SplunkPushDownRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  protected SplunkPushDownRule(RelOptRuleOperand operand, String id) {
    this(ImmutableSplunkPushDownRule.Config.builder()
        .withOperandSupplier(b -> b.exactly(operand))
        .build()
        .withId(id));
  }

  @Deprecated // to be removed before 2.0
  protected SplunkPushDownRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String id) {
    this(ImmutableSplunkPushDownRule.Config.builder()
        .withOperandSupplier(b -> b.exactly(operand))
        .withRelBuilderFactory(relBuilderFactory)
        .build()
        .withId(id));
  }

  // ~ Methods --------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    LOGGER.debug(description);

    int relLength = call.rels.length;
    SplunkTableScan splunkRel =
        (SplunkTableScan) call.rels[relLength - 1];

    LogicalFilter filter;
    LogicalProject topProj = null;
    LogicalProject bottomProj = null;


    RelDataType topRow = splunkRel.getRowType();

    int filterIdx = 2;
    if (call.rels[relLength - 2] instanceof LogicalProject) {
      bottomProj = (LogicalProject) call.rels[relLength - 2];
      filterIdx = 3;

      // bottom projection will change the field count/order
      topRow = bottomProj.getRowType();
    }

    String filterString;

    if (filterIdx <= relLength
        && call.rels[relLength - filterIdx] instanceof LogicalFilter) {
      filter = (LogicalFilter) call.rels[relLength - filterIdx];

      int topProjIdx = filterIdx + 1;
      if (topProjIdx <= relLength
          && call.rels[relLength - topProjIdx] instanceof LogicalProject) {
        topProj = (LogicalProject) call.rels[relLength - topProjIdx];
      }

      RexCall filterCall = (RexCall) filter.getCondition();
      SqlOperator op = filterCall.getOperator();
      List<RexNode> operands = filterCall.getOperands();

      LOGGER.debug("fieldNames: {}", getFieldsString(topRow));

      final StringBuilder buf = new StringBuilder();
      if (getFilter(op, operands, buf, topRow.getFieldNames())) {
        filterString = buf.toString();
      } else {
        LOGGER.debug("Cannot push down filter: {} with operands: {}", op.getKind(),
                operands.size());
        return; // can't handle
      }
    } else {
      filterString = "";
    }

    // top projection will change the field count/order
    if (topProj != null) {
      topRow = topProj.getRowType();
    }
    LOGGER.debug("pre transformTo fieldNames: {}", getFieldsString(topRow));

    call.transformTo(
        appendSearchString(
            filterString, splunkRel, topProj, bottomProj,
            topRow, null));
  }

  /**
   * Appends a search string.
   *
   * @param toAppend   Search string to append
   * @param splunkRel  Relational expression
   * @param topProj    Top projection
   * @param bottomProj Bottom projection
   */
  protected RelNode appendSearchString(
      String toAppend,
      SplunkTableScan splunkRel,
      LogicalProject topProj,
      LogicalProject bottomProj,
      RelDataType topRow,
      RelDataType bottomRow) {
    final RelOptCluster cluster = splunkRel.getCluster();
    StringBuilder updateSearchStr = new StringBuilder(splunkRel.search);

    if (!toAppend.isEmpty()) {
      updateSearchStr.append(" ").append(toAppend);
    }
    List<RelDataTypeField> bottomFields =
        bottomRow == null ? null : bottomRow.getFieldList();
    List<RelDataTypeField> topFields =
        topRow == null ? null : topRow.getFieldList();

    if (bottomFields == null) {
      bottomFields = splunkRel.getRowType().getFieldList();
    }

    // handle bottom projection (ie choose a subset of the table fields)
    if (bottomProj != null) {
      List<RelDataTypeField> tmp = new ArrayList<>();
      List<RelDataTypeField> dRow = bottomProj.getRowType().getFieldList();
      for (RexNode rn : bottomProj.getProjects()) {
        RelDataTypeField rdtf;
        if (rn instanceof RexSlot) {
          RexSlot rs = (RexSlot) rn;
          rdtf = bottomFields.get(rs.getIndex());
        } else {
          rdtf = dRow.get(tmp.size());
        }
        tmp.add(rdtf);
      }
      bottomFields = tmp;
    }

    // field renaming: to -> from
    final PairList<String, String> renames = PairList.of();

    // handle top projection (ie reordering and renaming)
    List<RelDataTypeField> newFields = bottomFields;
    if (topProj != null) {
      LOGGER.debug("topProj: {}", topProj.getPermutation());
      newFields = new ArrayList<>();
      int i = 0;
      for (RexNode rn : topProj.getProjects()) {
        RexInputRef rif = (RexInputRef) rn;
        RelDataTypeField field = bottomFields.get(rif.getIndex());
        if (!bottomFields.get(rif.getIndex()).getName()
            .equals(topFields.get(i).getName())) {
          renames.add(bottomFields.get(rif.getIndex()).getName(),
              topFields.get(i).getName());
          field = topFields.get(i);
        }
        newFields.add(field);
      }
    }

    if (!renames.isEmpty()) {
      updateSearchStr.append("| rename ");
      renames.forEach((left, right) ->
          updateSearchStr.append(left).append(" AS ")
              .append(right).append(" "));
    }

    RelDataType resultType =
        cluster.getTypeFactory().createStructType(newFields);
    String searchWithFilter = updateSearchStr.toString();

    RelNode rel =
        new SplunkTableScan(
            cluster,
            splunkRel.getTable(),
            splunkRel.splunkTable,
            searchWithFilter,
            splunkRel.earliest,
            splunkRel.latest,
            resultType.getFieldNames());

    LOGGER.debug("end of appendSearchString fieldNames: {}",
        rel.getRowType().getFieldNames());
    return rel;
  }

  // ~ Private Methods ------------------------------------------------------

  /**
   * Enhanced asd method that handles polymorphic operands in comparisons.
   * This version is forgiving and tries to convert operands to compatible formats.
   */
  private static boolean asd(boolean like, List<RexNode> operands, StringBuilder s,
      List<String> fieldNames, int i) {
    RexNode operand = operands.get(i);

    if (operand instanceof RexCall) {
      s.append("(");
      final RexCall call = (RexCall) operand;
      boolean b = getFilter(call.getOperator(), call.getOperands(), s, fieldNames);
      if (!b) {
        return false;
      }
      s.append(")");
    } else {
      if (operand instanceof RexInputRef) {
        // Field reference
        int fieldIndex = ((RexInputRef) operand).getIndex();
        if (fieldIndex >= fieldNames.size()) {
          LOGGER.debug("Field index {} out of bounds for field list size: {}", fieldIndex,
              fieldNames.size());
          return false;
        }
        String name = fieldNames.get(fieldIndex);
        s.append(name);
      } else if (operand instanceof RexLiteral) {
        // Literal value - use forgiving conversion
        String tmp = toStringForgiving(like, (RexLiteral) operand, fieldNames, operands, i);
        if (tmp == null) {
          LOGGER.debug("Cannot convert literal to string: {}", operand);
          return false;
        }
        s.append(tmp);
      } else {
        LOGGER.debug("Unsupported operand type: {}", operand.getClass().getSimpleName());
        return false;
      }
    }
    return true;
  }

  /**
   * Forgiving toString method that considers context from the comparison.
   * This method tries to determine the best format based on what field it's being compared against.
   */
  private static String toStringForgiving(boolean like, RexLiteral literal,
      List<String> fieldNames, List<RexNode> allOperands, int currentIndex) {

    Object literalValue = literal.getValue();
    SqlTypeName literalType = literal.getTypeName();

    LOGGER.debug("Processing literal in comparison: type={}, valueClass={}, value={}",
        literalType,
        literalValue != null ? literalValue.getClass().getName() : "null",
        literalValue);

    // Try to determine if we're comparing against a time field
    String comparisonFieldName = getComparisonFieldName(allOperands, currentIndex, fieldNames);
    boolean isTimeComparison = isTimeField(comparisonFieldName);

    LOGGER.debug("Comparison context: field={}, isTimeField={}", comparisonFieldName,
        isTimeComparison);

    // If this looks like a timestamp and we're comparing against a time field,
    // convert to epoch seconds regardless of the literal's declared type
    if (isTimeComparison && (SqlTypeName.DATETIME_TYPES.contains(literalType) || isLikelyTimestamp(literalValue))) {
      try {
        long epochSeconds = convertToEpochSeconds(literalValue);
        LOGGER.debug("Converted timestamp for time field comparison: {}", epochSeconds);
        return String.valueOf(epochSeconds);
      } catch (Exception e) {
        LOGGER.debug("Failed to convert to timestamp for time comparison: {}", literalValue, e);
        // Fall through to standard processing
      }
    }

    // For non-time fields or non-timestamp values, use standard processing
    return toStringStandard(like, literal);
  }

  /**
   * Tries to determine what field this literal is being compared against.
   */
  private static String getComparisonFieldName(List<RexNode> operands, int currentIndex,
      List<String> fieldNames) {
    // In a binary comparison, if we're processing operand 1,
    // operand 0 is likely the field being compared
    if (currentIndex == 1 && operands.size() >= 2) {
      RexNode otherOperand = operands.get(0);
      if (otherOperand instanceof RexInputRef) {
        int fieldIndex = ((RexInputRef) otherOperand).getIndex();
        if (fieldIndex < fieldNames.size()) {
          return fieldNames.get(fieldIndex);
        }
      }
    }

    // If we're processing operand 0, operand 1 might be the field
    if (currentIndex == 0 && operands.size() >= 2) {
      RexNode otherOperand = operands.get(1);
      if (otherOperand instanceof RexInputRef) {
        int fieldIndex = ((RexInputRef) otherOperand).getIndex();
        if (fieldIndex < fieldNames.size()) {
          return fieldNames.get(fieldIndex);
        }
      }
    }

    return null;
  }

  /**
   * Determines if a field name represents a time/timestamp field.
   */
  private static boolean isTimeField(String fieldName) {
    if (fieldName == null) {
      return false;
    }

    String lowerFieldName = fieldName.toLowerCase();
    return lowerFieldName.equals("_time") ||
        lowerFieldName.contains("time") ||
        lowerFieldName.contains("timestamp") ||
        lowerFieldName.contains("date") ||
        lowerFieldName.equals("created_at") ||
        lowerFieldName.equals("updated_at") ||
        lowerFieldName.endsWith("_at") ||
        lowerFieldName.endsWith("_date") ||
        lowerFieldName.endsWith("_time");
  }

  /**
   * Standard toString processing for non-contextual cases.
   */
  private static String toStringStandard(boolean like, RexLiteral literal) {
    Object literalValue = literal.getValue();
    SqlTypeName litSqlType = literal.getTypeName();

    if (literalValue == null) {
      return null;
    }

    if (SqlTypeName.NUMERIC_TYPES.contains(litSqlType)) {
      return literalValue.toString();
    } else if (SqlTypeName.STRING_TYPES.contains(litSqlType)) {
      String value;
      if (literalValue instanceof NlsString) {
        value = ((NlsString) literalValue).getValue();
      } else {
        value = literalValue.toString();
      }
      if (like) {
        value = value.replace("%", "*");
      }
      return searchEscape(value);
    } else if (SqlTypeName.DATETIME_TYPES.contains(litSqlType)) {
      // For datetime types, try to convert to epoch seconds
      try {
        long epochSeconds = convertToEpochSeconds(literalValue);
        return String.valueOf(epochSeconds);
      } catch (Exception e) {
        LOGGER.debug("Failed to convert datetime literal: {}", literalValue, e);
        return searchEscape(literalValue.toString());
      }
    } else if (litSqlType == SqlTypeName.BOOLEAN) {
      return literalValue.toString();
    } else if (SqlTypeName.BINARY_TYPES.contains(litSqlType)) {
      return searchEscape(literalValue.toString());
    } else {
      return searchEscape(literalValue.toString());
    }
  }

  /**
   * Converts any RexLiteral to appropriate Splunk format, handling multiple input types.
   * This method is polymorphic - it doesn't care whether StatementPreparer converted
   * timestamps or not.
   */
  private static String convertToSplunkFormat(RexLiteral literal) {
    Object literalValue = literal.getValue();
    SqlTypeName literalType = literal.getTypeName();

    LOGGER.debug("Converting literal for CAST: type={}, valueClass={}, value={}",
        literalType,
        literalValue != null ? literalValue.getClass().getName() : "null",
        literalValue);

    if (literalValue == null) {
      return null;
    }

    // Handle timestamp-related casts - accept multiple input formats
    if (SqlTypeName.DATETIME_TYPES.contains(literalType) || isLikelyTimestamp(literalValue)) {
      try {
        long epochSeconds = convertToEpochSeconds(literalValue);
        LOGGER.debug("Converted to epoch seconds: {}", epochSeconds);
        return String.valueOf(epochSeconds);
      } catch (Exception e) {
        LOGGER.debug("Failed to convert to timestamp: {}", literalValue, e);
        return null;
      }
    }

    // Handle other data types
    if (SqlTypeName.NUMERIC_TYPES.contains(literalType)) {
      return literalValue.toString();
    }

    if (SqlTypeName.STRING_TYPES.contains(literalType)) {
      String stringValue;
      if (literalValue instanceof NlsString) {
        stringValue = ((NlsString) literalValue).getValue();
      } else {
        stringValue = literalValue.toString();
      }
      return searchEscape(stringValue);
    }

    // Default: convert to string and escape
    return searchEscape(literalValue.toString());
  }

  /**
   * Determines if a value is likely a timestamp, regardless of its Java type.
   */
  private static boolean isLikelyTimestamp(Object value) {
    if (value instanceof java.sql.Timestamp ||
        value instanceof java.sql.Date ||
        value instanceof java.util.Date ||
        value instanceof java.util.Calendar) {
      return true;
    }

    if (value instanceof Long) {
      // Could be epoch milliseconds or seconds
      long longValue = (Long) value;
      return longValue > 946684800L && longValue < 4102444800000L; // Year 2000-2100 range
    }

    if (value instanceof String || value instanceof NlsString) {
      String strValue = value instanceof NlsString ?
          ((NlsString) value).getValue() : value.toString();
      return isTimestampString(strValue);
    }

    return false;
  }

  /**
   * Converts any timestamp representation to epoch seconds.
   * Handles: String, java.sql.Timestamp, java.sql.Date, Long, Integer, Calendar
   */
  private static long convertToEpochSeconds(Object timestampValue) throws Exception {
    if (timestampValue instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) timestampValue).getTime() / 1000;
    }

    if (timestampValue instanceof java.sql.Date) {
      return ((java.sql.Date) timestampValue).getTime() / 1000;
    }

    if (timestampValue instanceof java.util.Date) {
      return ((java.util.Date) timestampValue).getTime() / 1000;
    }

    if (timestampValue instanceof java.util.Calendar) {
      return ((java.util.Calendar) timestampValue).getTimeInMillis() / 1000;
    }

    if (timestampValue instanceof Long) {
      long longValue = (Long) timestampValue;
      // Determine if it's milliseconds or seconds
      if (longValue > 9999999999L) { // milliseconds
        return longValue / 1000;
      } else { // seconds
        return longValue;
      }
    }

    if (timestampValue instanceof Integer) {
      // Could be epoch seconds or days since epoch
      return ((Integer) timestampValue).longValue();
    }

    // Handle string representations
    String strValue;
    if (timestampValue instanceof NlsString) {
      strValue = ((NlsString) timestampValue).getValue();
    } else {
      strValue = timestampValue.toString();
    }

    return parseTimestampToEpochSeconds(strValue);
  }

  /**
   * Checks if a string looks like a timestamp using the same pattern as StatementPreparer.
   */
  private static boolean isTimestampString(String str) {
    if (str == null) {
      return false;
    }

    // Reuse the same pattern from StatementPreparer (line 75)
    Pattern utcPattern = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}([T\\s]\\d{2}:\\d{2}:\\d{2}(\\" +
        ".\\d{3}))Z$");

    return utcPattern.matcher(str).matches() ||
        str.matches("\\d{4}-\\d{2}-\\d{2}") ||           // Date only
        str.matches("\\d{10,13}");                       // Epoch seconds/milliseconds
  }

  private static long parseTimestampToEpochSeconds(String timestampStr) throws Exception {
    // Handle epoch seconds/milliseconds first
    if (timestampStr.matches("\\d{10,13}")) {
      long value = Long.parseLong(timestampStr);
      if (value > 9999999999L) { // milliseconds
        return value / 1000;
      } else { // seconds
        return value;
      }
    }

    // Use the same RFC formatters as StatementPreparer
    java.time.format.DateTimeFormatter rfcFormatter =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    java.time.format.DateTimeFormatter rfc3339Formatter =
        new java.time.format.DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .optionalStart()
            .appendPattern(".SSS")
            .optionalEnd()
            .appendPattern("XXX")
            .toFormatter();

    try {
      // Try RFC formatters first (matching StatementPreparer logic)
      try {
        java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(timestampStr, rfcFormatter);
        return zdt.toEpochSecond();
      } catch (Exception e1) {
        try {
          java.time.ZonedDateTime zdt =
              java.time.ZonedDateTime.parse(timestampStr, rfc3339Formatter);
          return zdt.toEpochSecond();
        } catch (Exception e2) {
          // Try standard ISO formatters as fallback
          try {
            return java.time.ZonedDateTime.parse(timestampStr,
                java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME).toEpochSecond();
          } catch (Exception e3) {
            // Try as local datetime and assume UTC
            return java.time.LocalDateTime.parse(timestampStr.replace(" ", "T"),
                    java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .atZone(java.time.ZoneOffset.UTC).toEpochSecond();
          }
        }
      }
    } catch (Exception e) {
      throw new Exception("Failed to parse timestamp string: " + timestampStr, e);
    }
  }

  @SuppressWarnings("unused")
  private static RelNode addProjectionRule(LogicalProject proj, RelNode rel) {
    if (proj == null) {
      return rel;
    }
    return LogicalProject.create(rel, proj.getHints(),
        proj.getProjects(), proj.getRowType(), proj.getVariablesSet());
  }

  // TODO: use StringBuilder instead of String
  // TODO: refactor this to use more tree like parsing, need to also
  //      make sure we use parens properly - currently precedence
  //      rules are simply left to right
  private static boolean getFilter(SqlOperator op, List<RexNode> operands,
      StringBuilder s, List<String> fieldNames) {
    if (!valid(op.getKind())) {
      LOGGER.debug("Unsupported operator: {}", op.getKind());
      return false;
    }

    boolean like = false;
    switch (op.getKind()) {
    case NOT:
      // NOT op pre-pended
      s.append(" NOT ");
      break;
    case CAST:
      // Handle CAST operations by converting the operand to appropriate Splunk format
      if (operands.size() < 1) {
        LOGGER.debug("CAST operation requires at least 1 operand, got: {}", operands.size());
        return false;
      }

      RexNode valueOperand = operands.get(0);

      if (valueOperand instanceof RexLiteral) {
        // Handle literal values - could be strings, timestamps, or other types
        String convertedValue = convertToSplunkFormat((RexLiteral) valueOperand);
        if (convertedValue != null) {
          s.append(convertedValue);
          return true;
        } else {
          LOGGER.debug("Failed to convert CAST literal to Splunk format");
          return false;
        }
      }

      // For non-literal operands (expressions, field references), just process normally
      return asd(false, operands, s, fieldNames, 0);
    case LIKE:
      like = true;
      break;
    case IS_NULL:
      // Handle IS NULL - only process the field operand
      if (operands.size() != 1) {
        LOGGER.debug("IS_NULL expects exactly 1 operand, got: {}", operands.size());
        return false;
      }
      if (!asd(false, operands, s, fieldNames, 0)) {
        return false;
      }
      s.append(" IS NULL");
      return true;
    case IS_NOT_NULL:
      // Handle IS NOT NULL - only process the field operand
      if (operands.size() != 1) {
        LOGGER.debug("IS_NOT_NULL expects exactly 1 operand, got: {}", operands.size());
        return false;
      }
      if (!asd(false, operands, s, fieldNames, 0)) {
        return false;
      }
      s.append(" IS NOT NULL");
      return true;
    case IN:
      // Handle IN operator
      if (operands.size() < 2) {
        LOGGER.debug("IN expects at least 2 operands, got: {}", operands.size());
        return false;
      }
      // First operand is the field
      if (!asd(false, operands, s, fieldNames, 0)) {
        return false;
      }
      s.append(" IN (");
      // Remaining operands are the values
      for (int i = 1; i < operands.size(); i++) {
        if (i > 1) {
          s.append(", ");
        }
        if (!asd(false, operands, s, fieldNames, i)) {
          return false;
        }
      }
      s.append(")");
      return true;
    default:
      break;
    }

    // Process binary operators (EQUALS, LESS_THAN, etc.)
    for (int i = 0; i < operands.size(); i++) {
      if (!asd(like, operands, s, fieldNames, i)) {
        return false;
      }
      if (op instanceof SqlBinaryOperator && i == 0) {
        s.append(" ").append(getSplunkOperator(op)).append(" ");
      }
    }
    return true;
  }

  private static boolean valid(SqlKind kind) {
    return SUPPORTED_OPS.contains(kind);
  }

  /**
   * Converts SQL operators to Splunk-compatible operators.
   */
  @SuppressWarnings("unused")
  private static String toString(SqlOperator op) {
    if (op.equals(SqlStdOperatorTable.NOT_EQUALS)) {
      return "!=";
    }
    return op.toString();
  }

  /**
   * Gets the Splunk-compatible operator string.
   */
  private static String getSplunkOperator(SqlOperator op) {
    if (op.equals(SqlStdOperatorTable.NOT_EQUALS)) {
      return "!=";
    }
    return op.toString();
  }

  public static String searchEscape(String str) {
    if (str.isEmpty()) {
      return "\"\"";
    }
    StringBuilder sb = new StringBuilder(str.length());
    boolean quote = false;

    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '"' || c == '\\') {
        sb.append('\\');
      }
      sb.append(c);

      quote |= !(Character.isLetterOrDigit(c) || c == '_');
    }

    if (quote || sb.length() != str.length()) {
      sb.insert(0, '"');
      sb.append('"');
      return sb.toString();
    }
    return str;
  }

  /**
   * Converts a RexLiteral to a string representation suitable for Splunk queries.
   * Handles nullable types and all SQL data types.
   */
  private static String toString(boolean like, RexLiteral literal) {
    String value = null;
    SqlTypeName litSqlType = literal.getTypeName();

    if (SqlTypeName.NUMERIC_TYPES.contains(litSqlType)) {
      // Handle all numeric types: INTEGER, BIGINT, DECIMAL, DOUBLE, FLOAT, etc.
      Object literalValue = literal.getValue();
      if (literalValue != null) {
        value = literalValue.toString();
      }
    } else if (SqlTypeName.STRING_TYPES.contains(litSqlType)) {
      // Handle all string types: CHAR, VARCHAR, LONGVARCHAR, CLOB, NCHAR, NVARCHAR,
      // LONGNVARCHAR, NCLOB
      Object literalValue = literal.getValue();
      if (literalValue instanceof NlsString) {
        value = ((NlsString) literalValue).getValue();
      } else if (literalValue != null) {
        value = literalValue.toString();
      } else {
        return null; // null literal value
      }
      if (like) {
        value = value.replace("%", "*");
      }
      value = searchEscape(value);
    } else if (SqlTypeName.DATETIME_TYPES.contains(litSqlType)) {
      // Handle DATE, TIME, TIMESTAMP, etc.
      Object literalValue = literal.getValue();
      if (literalValue != null) {
        if (literalValue instanceof java.sql.Timestamp) {
          // Convert SQL Timestamp to epoch seconds for Splunk (as numeric, not escaped)
          value = String.valueOf(((java.sql.Timestamp) literalValue).getTime() / 1000);
        } else if (literalValue instanceof java.sql.Date) {
          // Convert SQL Date to epoch seconds
          value = String.valueOf(((java.sql.Date) literalValue).getTime() / 1000);
        } else if (literalValue instanceof Long) {
          // Already epoch milliseconds - convert to seconds
          value = String.valueOf(((Long) literalValue) / 1000);
        } else if (literalValue instanceof Integer) {
          // Might be epoch seconds or days since epoch
          Integer intValue = (Integer) literalValue;
          if (litSqlType == SqlTypeName.DATE) {
            // Days since epoch - convert to epoch seconds
            value = String.valueOf(intValue * 24L * 60L * 60L);
          } else {
            // Assume it's already in seconds
            value = intValue.toString();
          }
        } else {
          // Fallback: try to parse as string and convert
          String strValue = literalValue.toString();
          try {
            // Try parsing as epoch milliseconds first
            long epochMs = Long.parseLong(strValue);
            value = String.valueOf(epochMs / 1000);
          } catch (NumberFormatException e) {
            // If it's not a number, treat as string and escape
            value = searchEscape(strValue);
          }
        }
        // Note: Don't escape numeric timestamp values - they should remain as numbers
      }
    } else if (litSqlType == SqlTypeName.BOOLEAN) {
      // Handle BOOLEAN type
      Object literalValue = literal.getValue();
      if (literalValue != null) {
        value = literalValue.toString();
      }
    } else if (SqlTypeName.BINARY_TYPES.contains(litSqlType)) {
      // Handle BINARY, VARBINARY - might need special encoding for Splunk
      Object literalValue = literal.getValue();
      if (literalValue != null) {
        value = literalValue.toString();
        value = searchEscape(value);
      }
    } else {
      // Handle any other types by converting to string
      Object literalValue = literal.getValue();
      if (literalValue != null) {
        value = literalValue.toString();
        value = searchEscape(value);
      }
    }

    return value;
  }

  // transform the call from SplunkUdxRel to FarragoJavaUdxRel
  // usually used to stop the optimizer from calling us
  protected void transformToFarragoUdxRel(
      RelOptRuleCall call,
      SplunkTableScan splunkRel,
      LogicalFilter filter,
      LogicalProject topProj,
      LogicalProject bottomProj) {
    assert false;
/*
    RelNode rel =
        new EnumerableTableScan(
            udxRel.getCluster(),
            udxRel.getTable(),
            udxRel.getRowType(),
            udxRel.getServerMofId());

    rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

    rel = addProjectionRule(bottomProj, rel);

    if (filter != null) {
      rel =
          new LogicalFilter(filter.getCluster(), rel, filter.getCondition());
    }

    rel = addProjectionRule(topProj, rel);

    call.transformTo(rel);
*/
  }

  public static String getFieldsString(RelDataType row) {
    return row.getFieldNames().toString();
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    @Override default SplunkPushDownRule toRule() {
      return new SplunkPushDownRule(this);
    }

    /**
     * Defines an operand tree for the given classes.
     */
    default Config withOperandFor(Class<? extends RelNode> relClass) {
      return withOperandSupplier(b -> b.operand(relClass).anyInputs())
          .as(Config.class);
    }

    default Config withId(String id) {
      return withDescription("SplunkPushDownRule: " + id).as(Config.class);
    }
  }
}
