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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Enumerator that evaluates Arrow filter tokens in Java.
 */
class ArrowFilterEnumerator extends AbstractArrowEnumerator {
  private final List<List<ConditionToken>> conditions;
  private final Schema schema;
  private final Runnable onClose;
  private final List<ValueVector> filterVectors;
  private final Map<String, Pattern> likePatterns;

  ArrowFilterEnumerator(ArrowFileReader arrowFileReader,
      ImmutableIntList fields, List<List<List<String>>> conditions,
      Schema schema, Runnable onClose) {
    super(arrowFileReader, fields);
    this.conditions = toConditionTokens(conditions);
    this.schema = schema;
    this.onClose = onClose;
    this.filterVectors = new ArrayList<>(schema.getFields().size());
    this.likePatterns = new HashMap<>();
  }

  @Override protected void loadNextArrowBatch() {
    super.loadNextArrowBatch();
    final VectorSchemaRoot root;
    try {
      root = arrowFileReader.getVectorSchemaRoot();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
    filterVectors.clear();
    for (int i = 0; i < schema.getFields().size(); i++) {
      filterVectors.add(root.getVector(i));
    }
  }

  @Override public boolean moveNext() {
    while (true) {
      if (currRowIndex >= rowCount - 1) {
        if (!loadNextNonEmptyArrowBatch()) {
          return false;
        }
      }
      currRowIndex++;
      if (matches(currRowIndex)) {
        return true;
      }
    }
  }

  private boolean matches(int rowIndex) {
    for (List<ConditionToken> orGroup : conditions) {
      boolean any = false;
      for (ConditionToken token : orGroup) {
        if (matches(token, rowIndex)) {
          any = true;
          break;
        }
      }
      if (!any) {
        return false;
      }
    }
    return true;
  }

  private boolean matches(ConditionToken token, int rowIndex) {
    final Object value = getValue(fieldVector(token.fieldName), rowIndex);
    switch (token.operator) {
    case IS_NULL:
      return value == null;
    case IS_NOT_NULL:
      return value != null;
    case IS_TRUE:
      return Boolean.TRUE.equals(value);
    case IS_FALSE:
      return Boolean.FALSE.equals(value);
    case IS_NOT_TRUE:
      return !Boolean.TRUE.equals(value);
    case IS_NOT_FALSE:
      return !Boolean.FALSE.equals(value);
    case EQUAL:
      return value != null && compare(value, literal(token)) == 0;
    case NOT_EQUAL:
      return value != null && compare(value, literal(token)) != 0;
    case LESS_THAN:
      return value != null && compare(value, literal(token)) < 0;
    case LESS_THAN_OR_EQUAL:
      return value != null && compare(value, literal(token)) <= 0;
    case GREATER_THAN:
      return value != null && compare(value, literal(token)) > 0;
    case GREATER_THAN_OR_EQUAL:
      return value != null && compare(value, literal(token)) >= 0;
    case LIKE:
      return value != null
          && like(value.toString(), requireNonNull(token.value, "value"));
    default:
      throw new AssertionError("Unhandled Arrow filter operator: " + token.operator);
    }
  }

  private ValueVector fieldVector(String fieldName) {
    final Field field = schema.findField(fieldName);
    final int index = schema.getFields().indexOf(field);
    if (index < 0) {
      throw new IllegalArgumentException("Unknown Arrow field: " + fieldName);
    }
    return filterVectors.get(index);
  }

  private static Object literal(ConditionToken token) {
    final String type = requireNonNull(token.valueType, "valueType");
    final String value = requireNonNull(token.value, "value");
    if (type.startsWith("decimal")) {
      return new BigDecimal(value);
    } else if (type.equals("integer")) {
      return Integer.valueOf(value);
    } else if (type.equals("long")) {
      return Long.valueOf(value);
    } else if (type.equals("float")) {
      return Float.valueOf(value);
    } else if (type.equals("double")) {
      return Double.valueOf(value);
    } else if (type.equals("string")) {
      return unquote(value);
    }
    throw new UnsupportedOperationException("Unsupported literal type: " + type);
  }

  private static int compare(Object left, Object right) {
    if (left instanceof BigDecimal || right instanceof BigDecimal) {
      return toBigDecimal(left).compareTo(toBigDecimal(right));
    }
    if (left instanceof Number && right instanceof Number) {
      return Double.compare(((Number) left).doubleValue(),
          ((Number) right).doubleValue());
    }
    return left.toString().compareTo(right.toString());
  }

  private static BigDecimal toBigDecimal(Object value) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    return new BigDecimal(value.toString());
  }

  private boolean like(String value, String pattern) {
    final String unquotedPattern = unquote(pattern);
    final Pattern compiledPattern =
        likePatterns.computeIfAbsent(unquotedPattern, p -> {
          return Pattern.compile(toRegex(p), Pattern.DOTALL);
        });
    return compiledPattern.matcher(value).matches();
  }

  private static String toRegex(String pattern) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      final char c = pattern.charAt(i);
      if (c == '%') {
        builder.append(".*");
      } else if (c == '_') {
        builder.append('.');
      } else {
        builder.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return builder.toString();
  }

  private static String unquote(String value) {
    if (value.length() >= 2 && value.charAt(0) == '\''
        && value.charAt(value.length() - 1) == '\'') {
      return value.substring(1, value.length() - 1).replace("''", "'");
    }
    return value;
  }

  private static List<List<ConditionToken>> toConditionTokens(
      List<List<List<String>>> conditions) {
    final List<List<ConditionToken>> result =
        new ArrayList<>(conditions.size());
    for (List<List<String>> orGroup : conditions) {
      final List<ConditionToken> tokens = new ArrayList<>(orGroup.size());
      for (List<String> token : orGroup) {
        tokens.add(ConditionToken.fromTokenList(token));
      }
      result.add(tokens);
    }
    return result;
  }

  @Override public void close() {
    onClose.run();
  }
}
