/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.calcite.slt;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

public class SqlTestQueryOutputDescription {
  public enum SortOrder {
    None,
    Row,
    Value
  }

  public int valueCount;
  /**
   * Encoded types of columns expected in result.
   */
  @Nullable
  public String columnTypes;
  @Nullable
  public String hash;
  /**
   * How results are sorted.
   */
  public SortOrder order;
  @Nullable
  public List<String> queryResults;

  public SqlTestQueryOutputDescription() {
    this.columnTypes = null;
    this.valueCount = 0;
    this.hash = null;
    this.queryResults = null;
    this.order = SortOrder.None;
  }

  public void clearResults() {
    this.queryResults = new ArrayList<>();
  }

  public void addResultLine(String line) {
    if (this.queryResults == null)
      throw new RuntimeException("queryResults were not initialized");
    this.queryResults.add(line);
    this.valueCount++;
  }

  /**
   * Parse the output type.
   *
   * @param line A string that starts with the output type.
   * @return The tail of the string or null on error.
   */
  @Nullable
  String parseType(String line) {
    int space = line.indexOf(" ");
    if (space < 0)
      throw new RuntimeException("No column types identified");
    this.columnTypes = line.substring(0, space).trim();
    for (int i = 0; i < this.columnTypes.length(); i++) {
      // Type of result encoded as characters.
      char c = line.charAt(i);
      switch (c) {
      case 'I':
      case 'R':
      case 'T':
        continue;
      default:
        throw new RuntimeException("Unexpected column type " + c);
      }
    }
    return line.substring(space + 1);
  }

  /**
   * Parse the sorting order
   *
   * @param orderDescription A String that starts with the ordering description
   * @return null on failure, the remaining string if the order is recognized.
   */
  @SuppressWarnings("SpellCheckingInspection")
  @Nullable
  String parseOrder(String orderDescription) {
    if (orderDescription.startsWith("nosort")) {
      this.order = SortOrder.None;
      return orderDescription.substring("nosort" .length());
    } else if (orderDescription.startsWith("rowsort")) {
      this.order = SortOrder.Row;
      return orderDescription.substring("rowsort" .length());
    } else if (orderDescription.startsWith("valuesort")) {
      this.order = SortOrder.Value;
      return orderDescription.substring("valuesort" .length());
    }
    return null;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public void setValueCount(int values) {
    this.valueCount = values;
  }

  /**
   * Return -1 if the output size is not known.
   */
  public int getExpectedOutputSize() {
    if (this.columnTypes == null || this.valueCount < 0)
      return -1;
    return this.valueCount / this.columnTypes.length();
  }
}
