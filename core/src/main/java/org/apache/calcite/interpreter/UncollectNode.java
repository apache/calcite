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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Uncollect}.
 */
public class UncollectNode extends AbstractSingleNode<Uncollect> {

  public UncollectNode(Compiler compiler, Uncollect uncollect) {
    super(compiler, uncollect);
  }

  @Override public void run() throws InterruptedException {
    final int inputFieldCount = rel.getInput().getRowType().getFieldCount();
    if (rel.getPassthroughFieldIndices().isEmpty()
        && rel.getCollectionFieldIndices().cardinality() == inputFieldCount) {
      runPlain();
    } else {
      runGeneralized();
    }
  }

  /** Unnests every input field; every input field must be a collection. */
  private void runPlain() throws InterruptedException {
    Row row = null;
    while ((row = source.receive()) != null) {
      for (Object value : row.getValues()) {
        if (value == null) {
          throw new NullPointerException("NULL value for unnest.");
        }
        int i = 1;
        if (value instanceof List) {
          List list = (List) value;
          for (Object o : list) {
            if (rel.withOrdinality) {
              sink.send(Row.of(o, i++));
            } else {
              sink.send(Row.of(o));
            }
          }
        } else if (value instanceof Map) {
          Map map = (Map) value;
          for (Object key : map.keySet()) {
            if (rel.withOrdinality) {
              sink.send(Row.of(key, map.get(key), i++));
            } else {
              sink.send(Row.of(key, map.get(key)));
            }
          }
        } else {
          throw new UnsupportedOperationException(
              String.format(Locale.ROOT,
                  "Invalid type: %s for unnest.",
                  value.getClass().getCanonicalName()));
        }
      }
    }
  }

  /**
   * Unnests the collection fields and prepends the passthrough fields,
   * mirroring {@code SqlFunctions.flatUncollect}: multiple collections zip
   * with {@code null} padding, a {@code null} collection counts as empty,
   * and when all collections are empty {@code isOuter} decides between one
   * {@code null}-padded row and dropping the input row.
   */
  private void runGeneralized() throws InterruptedException {
    final List<RelDataTypeField> inputFields = rel.getInput().getRowType().getFieldList();
    final List<Integer> passthroughs = rel.getPassthroughFieldIndices().toList();
    final List<Integer> collections = rel.getCollectionFieldIndices().toList();

    // Output width of each collection: 2 for maps (key, value), the component
    // field count for struct elements, otherwise 1.
    final int[] widths = new int[collections.size()];
    int elementWidth = 0;
    for (int c = 0; c < collections.size(); c++) {
      final RelDataType type = inputFields.get(collections.get(c)).getType();
      if (type.getSqlTypeName() == SqlTypeName.MAP) {
        widths[c] = 2;
      } else {
        final RelDataType component =
            requireNonNull(type.getComponentType(), "componentType");
        widths[c] = component.isStruct() ? component.getFieldCount() : 1;
      }
      elementWidth += widths[c];
    }
    final int outputWidth =
        passthroughs.size() + elementWidth + (rel.withOrdinality ? 1 : 0);

    Row row = null;
    while ((row = source.receive()) != null) {
      final Object[] values = row.getValues();
      final List<List<Object>> elements = new ArrayList<>();
      int maxSize = 0;
      for (int index : collections) {
        final Object value = values[index];
        final List<Object> list;
        if (value == null) {
          list = Collections.emptyList();
        } else if (value instanceof List) {
          list = (List<Object>) value;
        } else if (value instanceof Map) {
          list = new ArrayList<>(((Map<Object, Object>) value).entrySet());
        } else {
          throw new UnsupportedOperationException(
              String.format(Locale.ROOT,
                  "Invalid type: %s for unnest.",
                  value.getClass().getCanonicalName()));
        }
        elements.add(list);
        maxSize = Math.max(maxSize, list.size());
      }

      if (maxSize == 0) {
        if (rel.isOuter) {
          final Object[] out = new Object[outputWidth];
          int outCol = 0;
          for (int p : passthroughs) {
            out[outCol++] = values[p];
          }
          sink.send(Row.of(out));
        }
        continue;
      }

      for (int i = 0; i < maxSize; i++) {
        final Object[] out = new Object[outputWidth];
        int outCol = 0;
        for (int p : passthroughs) {
          out[outCol++] = values[p];
        }
        for (int c = 0; c < collections.size(); c++) {
          final List<Object> list = elements.get(c);
          final Object element = i < list.size() ? list.get(i) : null;
          outCol = appendElement(out, outCol, widths[c], element);
        }
        if (rel.withOrdinality) {
          out[outCol] = i + 1;
        }
        sink.send(Row.of(out));
      }
    }
  }

  /** Writes one collection element into {@code out} starting at {@code outCol},
   * expanding map entries and struct elements into their columns; a {@code null}
   * element leaves its columns {@code null}. Returns the next free column. */
  private static int appendElement(Object[] out, int outCol, int width,
      Object element) {
    if (element instanceof Map.Entry) {
      final Map.Entry<?, ?> entry = (Map.Entry<?, ?>) element;
      out[outCol] = entry.getKey();
      out[outCol + 1] = entry.getValue();
    } else if (width == 1 || element == null) {
      out[outCol] = element;
    } else if (element instanceof Object[]) {
      System.arraycopy((Object[]) element, 0, out, outCol, width);
    } else if (element instanceof List) {
      final List<?> struct = (List<?>) element;
      for (int i = 0; i < width; i++) {
        out[outCol + i] = struct.get(i);
      }
    } else {
      throw new UnsupportedOperationException(
          String.format(Locale.ROOT,
              "Invalid element type: %s for unnest.",
              element.getClass().getCanonicalName()));
    }
    return outCol + width;
  }
}
