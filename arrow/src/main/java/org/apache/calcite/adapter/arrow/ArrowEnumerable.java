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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Enumerable that reads from Arrow value-vectors.
 */
class ArrowEnumerable extends AbstractEnumerable<Object> {
  private final ArrowFileReader arrowFileReader;
  private final ImmutableIntList fields;
  private final List<List<List<String>>> conditions;
  private final Schema schema;
  private final Runnable onClose;

  ArrowEnumerable(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      List<List<List<String>>> conditions, Schema schema, Runnable onClose) {
    this.arrowFileReader = arrowFileReader;
    this.conditions = conditions;
    this.schema = schema;
    this.fields = fields;
    this.onClose = onClose;
  }

  @Override public Enumerator<Object> enumerator() {
    try {
      if (!conditions.isEmpty()) {
        return new ArrowFilterEnumerator(arrowFileReader, fields,
            conditions, schema, onClose);
      }
      // No projector and no filter means the query is an identity projection
      // that should read selected value-vectors directly.
      return new ArrowDirectEnumerator(arrowFileReader, fields, onClose);
    } catch (Exception e) {
      throw Util.toUnchecked(e);
    }
  }
}
