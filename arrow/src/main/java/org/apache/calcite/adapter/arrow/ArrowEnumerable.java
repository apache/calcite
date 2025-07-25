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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Enumerable that reads from Arrow value-vectors or Parquet files.
 */
class ArrowEnumerable extends AbstractEnumerable<Object> {
  private final Object sourceReader; // Can be ArrowFileReader or ParquetReader.Builder<Object>
  private final ImmutableIntList fields;
  private final @Nullable Object projector; // Can be Arrow Projector or Parquet ReadSupport
  private final @Nullable Object filter; // Can be Arrow Filter or Parquet FilterCompat.Filter

  ArrowEnumerable(Object sourceReader, ImmutableIntList fields,
      @Nullable Object projector, @Nullable Object filter) {
    this.sourceReader = sourceReader;
    this.projector = projector;
    this.filter = filter;
    this.fields = fields;
  }

  @Override public Enumerator<Object> enumerator() {
    try {
      if (projector != null) {
        return new ArrowProjectEnumerator(sourceReader, fields, projector);
      } else if (filter != null) {
        return new ArrowFilterEnumerator(sourceReader, fields, filter);
      }
      throw new IllegalArgumentException(
          "The enumerator must have either a filter or a projection");
    } catch (Exception e) {
      throw Util.toUnchecked(e);
    }
  }
}
