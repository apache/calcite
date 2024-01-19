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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.vector.ipc.ArrowFileReader;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Enumerable that reads from Arrow value-vectors.
 */
class ArrowEnumerable extends AbstractEnumerable<Object> {
  private final ArrowFileReader arrowFileReader;
  private final ImmutableIntList fields;
  private final @Nullable Projector projector;
  private final @Nullable Filter filter;


  ArrowEnumerable(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      @Nullable Projector projector, @Nullable Filter filter) {
    this.arrowFileReader = arrowFileReader;
    this.projector = projector;
    this.filter = filter;
    this.fields = fields;
  }

  @Override public Enumerator<Object> enumerator() {
    try {
      if (projector != null) {
        return new ArrowProjectEnumerator(arrowFileReader, fields, projector);
      } else if (filter != null) {
        return new ArrowFilterEnumerator(arrowFileReader, fields, filter);
      }
      throw new IllegalArgumentException(
          "The arrow enumerator must have either a filter or a projection");
    } catch (Exception e) {
      throw Util.toUnchecked(e);
    }
  }
}
