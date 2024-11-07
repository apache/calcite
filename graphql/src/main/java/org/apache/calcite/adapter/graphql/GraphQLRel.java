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

package org.apache.calcite.adapter.graphql;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Sarg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Range;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

/**
 * Interface representing a relational operation in the GraphQL context.
 */
public interface GraphQLRel extends RelNode {

  void implement(Implementor implementor);

  /**
   * Convention for GraphQL relational operations.
   */
  Convention CONVENTION = new Convention.Impl("GRAPHQL", GraphQLRel.class);

  /**
   * Callback for the implementation process.
   */
  class Implementor {

    public static final Map<String, String> KEYWORDS;
    static {
      Map<String, String> map = new HashMap<>();
      map.put("offset", "offset");
      map.put("limit", "limit");
      map.put("where", "where");
      map.put("order_by", "order_by");
      map.put("_not", "_not");
      map.put("_eq", "_eq");
      map.put("_gt", "_gt");
      map.put("_gte", "_gte");
      map.put("_lt", "_lt");
      map.put("_lte", "_lte");
      map.put("_and", "_and");
      map.put("_or", "_or");
      map.put("_in", "_in");
      KEYWORDS = map;
    }

    private static final Logger LOGGER = LogManager.getLogger(GraphQLRel.class);
    @Nullable private GraphQLTable graphQLTable;
    @Nullable public RelOptTable table;
    @Nullable List<Integer> selectFields;
    private final List<OrderByField> orderFields = new ArrayList<>();
    @Nullable private Integer offset = null;
    @Nullable private Integer fetch = null;
    @Nullable private RexNode filter = null;

    public void setGraphQLTable(GraphQLTable table) {
      this.graphQLTable = table;
    }

    /**
     * Adds order fields to the Implementor instance based on the provided RelCollation.
     *
     * @param collation The RelCollation specifying the order fields to be added
     */
    public void addOrder(RelCollation collation) {
      LOGGER.debug("Adding order fields from collation: {}", collation);

      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        int index = fieldCollation.getFieldIndex();
        boolean ascending = fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING;

        OrderByField orderField = new OrderByField(index, fieldCollation.getDirection());
        orderFields.add(orderField);

        LOGGER.debug("Added order field: field={}, ascending={}", index, ascending);
      }
    }

    /**
     * Returns the Integer value of a RexNode if it is a RexLiteral,
     * or null otherwise.
     *
     * @param node the RexNode to extract value from
     * @return the Integer value if node was a RexLiteral, or null otherwise
     */
    @Nullable
    private Integer getLiteralValue(RexNode node) {
      LOGGER.debug("Extracting literal value from RexNode: {}", node);
      if (node instanceof RexLiteral) {
        return ((RexLiteral) node).getValueAs(Integer.class);
      } else {
        LOGGER.warn("RexNode is not a literal: {}", node);
        return null;
      }
    }

    /**
     * Adds an offset to the Implementor instance based on the provided RexNode.
     *
     * @param offset The RexNode representing the offset value to be added
     */
    public void addOffset(RexNode offset) {
      LOGGER.debug("Adding offset: {}", offset);
      this.offset = getLiteralValue(offset);
      LOGGER.debug("Parsed offset value: {}", this.offset);
    }

    /**
     * Adds a fetch value to the Implementor instance based on the provided RexNode.
     *
     * @param fetch The RexNode representing the fetch value to be added
     */
    public void addFetch(RexNode fetch) {
      LOGGER.debug("Adding fetch: {}", fetch);
      this.fetch = getLiteralValue(fetch);
      LOGGER.debug("Parsed fetch value: {}", this.fetch);
    }

    /**
     * Adds a filter expression to the Implementor instance.
     *
     * @param filter The RexNode filter expression to be added
     */
    public void addFilter(RexNode filter) {
      this.filter = filter;
    }

    /**
     * Adds newly projected fields.
     *
     * @param fields New fields to be projected from a query
     */
    void addProjectFields(List<Integer> fields) {
      LOGGER.debug("addProjectFields called with fields: {}", fields);
      if (selectFields == null) {
        selectFields = ImmutableIntList.copyOf(fields);
        LOGGER.debug("Set initial selectFields to: {}", selectFields);
      } else {
        // For subsequent projections, we need to map through the previous projection
        List<Integer> newFields = new ArrayList<>();
        for (Integer field : fields) {
          if (field < selectFields.size()) {
            newFields.add(selectFields.get(field));
          }
        }
        selectFields = ImmutableIntList.copyOf(newFields);
        LOGGER.debug("Updated selectFields to: {}", selectFields);
      }
    }

    /**
     * Visits the input RelNode for processing and implementation.
     *
     * @param ordinal the ordinal position of the input
     * @param input the input RelNode to be visited
     */
    public void visitInput(int ordinal, RelNode input) {
      LOGGER.debug("visitInput called for ordinal {} with input {} type {}",
          ordinal, input, input.getClass().getName());
      ((GraphQLRel) input).implement(this);
      LOGGER.debug("After implementing input {}", input);
    }

    /**
     * Converts a given RexNode filter expression to a GraphQL filter syntax.
     *
     * @param filter   The RexNode filter expression to be converted
     * @param rowType  List of row types to be used in the conversion
     * @return A string representing the converted GraphQL filter in accordance with the input RexNode filter
     */
    private @Nullable String convertRexNodeToGraphQLFilter(@Nullable RexNode filter,
        List<String>  rowType) {
      // Implement this method to convert RexNode to GraphQL filter syntax
      // This is a placeholder and needs to be implemented based on your specific GraphQL schema
      // and requirements
      if (filter == null) {
        return "";
      }
      if (filter instanceof RexCall) {
        RexCall call = (RexCall) filter;
        switch (filter.getKind()) {
        case EQUALS:
          return String.format("{ %s: { %s: %s } }",
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_eq"),
              getComparator(call.operands));
        case NOT_EQUALS:
          return String.format("{ %s: { %s: { %s: %s } } }",
              KEYWORDS.get("_not"),
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_eq"),
              getComparator(call.operands));
        case GREATER_THAN:
          return String.format("{ %s: { %s: %s } }",
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_gt"),
              getComparator(call.operands));
        case GREATER_THAN_OR_EQUAL:
          return String.format("{ %s: { %s: %s } }",
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_gte"),
              getComparator(call.operands));
        case LESS_THAN:
          return String.format("{ %s: { %s: %s } }",
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_lt"),
              getComparator(call.operands));
        case LESS_THAN_OR_EQUAL:
          return String.format("{ %s: { %s: %s } }",
              getFieldName(call.operands, rowType),
              KEYWORDS.get("_lte"),
              getComparator(call.operands));
        case SEARCH:
          Object[] range = getRange(call.operands);
          if (range.length == 1 && !Objects.equals(((Range<?>) range[0]).lowerEndpoint().toString()
              , ((Range<?>) range[0]).upperEndpoint().toString())) {
            return String.format("{ %s: { %s: %s, %s: %s } }",
                getFieldName(call.operands, rowType),
                KEYWORDS.get("_gt"),
                ((Range<?>) range[0]).lowerEndpoint(),
                KEYWORDS.get("_lt"),
                ((Range<?>) range[0]).upperEndpoint()
            );
          } else {
            boolean hasLowerBound = ((Range<?>) range[0]).hasLowerBound();
            ArrayList<String> ranges = new ArrayList<>();
            for (Object r : range) {
              if (!hasLowerBound) {
                if (((Range<?>) r).hasUpperBound()) {
                  ranges.add(((Range<?>) r).upperEndpoint().toString());
                }
              } else {
                ranges.add(((Range<?>) r).lowerEndpoint().toString());
              }
            }
            String rangeString = String.join(",", ranges);
            if (!hasLowerBound) {
              return String.format("{ %s: { %s: { %s: [%s] } } }",
                  KEYWORDS.get("_not"),
                  getFieldName(call.operands, rowType),
                  KEYWORDS.get("_in"),
                  rangeString
              );
            }
            return String.format("{ %s: { %s: [%s] } }",
                getFieldName(call.operands, rowType),
                KEYWORDS.get("_in"),
                rangeString
            );
          }
        case NOT:
          return String.format("{ %s: %s }", KEYWORDS.get("_not"), convertRexNodeToGraphQLFilter(call.operands.get(1), rowType));
        case OR:
        case AND:
          StringBuilder f = new StringBuilder();
          switch (filter.getKind()) {
          case OR:
            f.append(String.format("{ %s: [", KEYWORDS.get("_and")));
            break;
          case AND:
            f.append(String.format("{ %s: [", KEYWORDS.get("_or")));
            break;
          }
          ArrayList<String> conditions = new ArrayList<>();
          for (RexNode o : ((RexCall) filter).operands) {
            String condition = convertRexNodeToGraphQLFilter(o, rowType);
            conditions.add(condition);
          }
          String conditionsString = String.join(",", conditions);
          f.append(conditionsString);
          f.append("]}");
          return f.toString();
        default:
          return null;
        }
      }
      return "";
    }

    private String getFieldName (List < RexNode > operands, List<String> rowType){
      Object opCandidate = operands.get(0);
      if (!(opCandidate instanceof RexInputRef)) {
        if (opCandidate instanceof RexCall) {
          RexCall call = (RexCall) opCandidate;
          if (call.op.getKind() == SqlKind.CAST) {
            return getFieldName(call.operands, rowType);
          }
        }
        throw new IllegalArgumentException("The first operand in a condition must be a column " +
            "name.");
      }
      RexInputRef op = (RexInputRef) opCandidate;
      return rowType.get(op.getIndex());
    }

    private @Nullable Object[] getRange (List < RexNode > operands){
      if (operands.size() < 1 + 1) {
        throw new IllegalArgumentException("Incorrect number of operands in a condition.");
      }
      Object opCandidate = operands.get(1);
      if (!(opCandidate instanceof RexLiteral)) {
        throw new IllegalArgumentException("The operand in a condition must be a literal.");
      }
      RexLiteral op = (RexLiteral) opCandidate;
      if (op.getValue() instanceof Sarg) {
        Sarg<?> sarg = (Sarg<?>) op.getValue();
        return sarg.rangeSet.asRanges().toArray();
      }
      return null;
    }

    private String getComparator (List < RexNode > operands){
      if (operands.size() < 1 + 1) {
        throw new IllegalArgumentException("Incorrect number of operands in a condition.");
      }
      Object opCandidate = operands.get(1);
      if (!(opCandidate instanceof RexLiteral)) {
        throw new IllegalArgumentException("The operand in a condition must be a literal.");
      }
      RexLiteral op = (RexLiteral) opCandidate;
      SqlTypeFamily sqlType = op.getTypeName().getFamily();
      if (sqlType == SqlTypeFamily.TIMESTAMP) {
        return String.format("\"%s\"", op.toString().replace(" ", "T"));
      }
      if (sqlType == SqlTypeFamily.DATE) {
        return String.format("\"%s\"", op);
      }
      return convertQuotes(((RexLiteral) opCandidate).toString());
    }

    private String convertQuotes (String input){
      //Replace any original double quotes to \"
      input = input.replace("\"", "\\\"");

      //If string starts and ends with single quote
      if (input.startsWith("'") && input.endsWith("'")) {
        //Replaces outer single quotes with double quotes
        input = "\"" + input.substring(1, input.length() - 1) + "\"";
      }
      return input;
    }
    public String getQuery(RelDataType rowType) {
      assert graphQLTable != null;
      StringBuilder builder = new StringBuilder(String.format("query find%s {\n",
          graphQLTable.getName()));
      List<String> fieldNames = GraphQLRules.graphQLFieldNames(rowType);
      builder.append("  ").append(graphQLTable.getSelectMany());
      List<String> orderBy = new ArrayList<>();
      assert table != null;
      List<String> fields = table.getRowType().getFieldNames();

      // Add ordering if present
      if (!orderFields.isEmpty()) {
        StringBuilder ob = new StringBuilder();
        ob.append(String.format("%s: {", KEYWORDS.get("order_by")));
        for (int i = 0; i < orderFields.size(); i++) {
          OrderByField item = orderFields.get(i);
          ob.append(item.toHasuraFormat());
          if (i > 0) {
            ob.append(", ");
          }
        }
        ob.append("}");
        orderBy.add(ob.toString());
      }
      if (offset != null) {
        orderBy.add(String.format("%s: %d", KEYWORDS.get("offset"), offset));
      }
      if (fetch != null) {
        orderBy.add(String.format("%s: %d", KEYWORDS.get("limit"), fetch));
      }
      if (filter != null) {
        orderBy.add(String.format("%s: %s", KEYWORDS.get("where"), convertRexNodeToGraphQLFilter(filter, fields)));
      }

      if (!orderBy.isEmpty()) {
        builder
            .append("(")
            .append(String.join(", ", orderBy))
            .append(")");
      }

      builder.append(" {\n");
      for (String field : fieldNames) {
        builder.append("    ").append(field).append("\n");
      }
      builder.append("  }\n");
      builder.append("}");
      return builder.toString();
    }
  }
}
