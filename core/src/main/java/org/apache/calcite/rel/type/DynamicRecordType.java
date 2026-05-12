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
package org.apache.calcite.rel.type;

import org.apache.calcite.util.Comment;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Specific type of RelRecordType that corresponds to a dynamic table,
 * where columns are created as they are requested.
 */
public abstract class DynamicRecordType extends RelDataTypeImpl {

  /** Default constructor; dynamic fields are held by the subclass. */
  protected DynamicRecordType() {
    super();
  }

  /**
   * Subclass constructor for dynamic record types that store fields outside of
   * {@link RelDataTypeImpl#fieldList} (for example in a {@link RelDataTypeHolder}).
   */
  protected DynamicRecordType(Set<Comment> comments,
      @Nullable List<? extends RelDataTypeField> fieldList) {
    super(comments, fieldList);
  }

  // The prefix string for dynamic star column name
  public static final String DYNAMIC_STAR_PREFIX = "**";

  @Override public boolean isDynamicStruct() {
    return true;
  }

  /**
   * Returns true if the column name starts with DYNAMIC_STAR_PREFIX.
   */
  public static boolean isDynamicStarColName(String name) {
    return name.startsWith(DYNAMIC_STAR_PREFIX);
  }

  @Override public abstract DynamicRecordType copy(Set<Comment> comments);

}
