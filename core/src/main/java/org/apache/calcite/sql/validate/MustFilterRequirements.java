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
package org.apache.calcite.sql.validate;

import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Class that encapsulates filtering requirements when overloading SemanticTable.
 *
 * <p>mustFilterFields: the ordinals (in the row type) of the "must-filter" fields,
 * fields that must be filtered in a query.
 * <p> mustFilterBypassFields: the ordinals (in the row type) of the "bypass" fields,
 * fields that can defuse validation errors on mustFilterFields if filtered on.
 * <p> remnantMustFilterFields: Set of mustFilterField SqlQualifieds that have not been defused
 * in the current query, but can still be defused by filtering on a bypass field in the
 * enclosing query.
 */
public class MustFilterRequirements {

  ImmutableBitSet mustFilterFields;
  ImmutableBitSet mustFilterBypassFields;
  protected Set<SqlQualified> remnantMustFilterFields;
  public MustFilterRequirements(ImmutableBitSet mustFilterFields,
      ImmutableBitSet mustFilterBypassFields, Set<SqlQualified> remnantMustFilterFields) {
    this.mustFilterFields = mustFilterFields;
    this.mustFilterBypassFields = mustFilterBypassFields;
    this.remnantMustFilterFields = remnantMustFilterFields;
  }

  public MustFilterRequirements() {
    this(ImmutableBitSet.of(), ImmutableBitSet.of(), ImmutableSet.of());
  }
}
