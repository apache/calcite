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
package org.apache.calcite.piglet;

import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents Pig Tuples with unknown fields. The tuple field
 * can only be accessed via name '$index', like ('$0', '$1').
 * The tuple is then resized to match the index.
 */
public class DynamicTupleRecordType extends DynamicRecordTypeImpl {
  private static final Pattern INDEX_PATTERN = Pattern.compile("^\\$(\\d+)$");

  DynamicTupleRecordType(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override public RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {
    final int index = nameToIndex(fieldName);
    if (index >= 0) {
      resize(index + 1);
      return super.getField(fieldName, caseSensitive, elideRecord);
    }
    return null;
  }

  /**
   * Resizes the record if the new size greater than the current size.
   *
   * @param size New size
   */
  void resize(int size) {
    int currentSize = getFieldCount();
    if (size > currentSize) {
      for (int i = currentSize; i < size; i++) {
        super.getField("$" + i, true, true);
      }
      computeDigest();
    }
  }

  /**
   * Gets index number from field name.
   * @param fieldName Field name, format example '$1'
   */
  private static int nameToIndex(String fieldName) {
    Matcher matcher = INDEX_PATTERN.matcher(fieldName);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    return -1;
  }
}

// End DynamicTupleRecordType.java
