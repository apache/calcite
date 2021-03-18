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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Rel Metadata cache back by @see HashBasedTable table.
 */
public class TableMetadataCache implements MetadataCache {
  public final Table<RelNode, Object, Object> map = HashBasedTable.create();

  /**
   * Removes cached metadata values for specified RelNode.
   *
   * @param rel RelNode whose cached metadata should be removed
   * @return true if cache for the provided RelNode was not empty
   */
  @Override public boolean clear(RelNode rel) {
    Map<Object, Object> row = map.row(rel);
    if (row.isEmpty()) {
      return false;
    }

    row.clear();
    return true;
  }

  @Override public @Nullable Object remove(RelNode relNode, Object args) {
    return map.remove(relNode, args);
  }

  @Override public @Nullable Object get(RelNode relNode, Object args) {
    return map.get(relNode, args);
  }

  @Override public @Nullable Object put(RelNode relNode, Object args, Object value) {
    return map.put(relNode, args, value);
  }
}
