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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.cascades.Enforcer;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

/**
 *
 */
public class CascadesTestSortEnforcer extends Enforcer<RelCollation> {
  public static final CascadesTestSortEnforcer INSTANCE = new CascadesTestSortEnforcer();

  public CascadesTestSortEnforcer() {
    super(RelCollationTraitDef.INSTANCE);
  }

  @Override public RelNode enforce(RelNode rel, RelCollation to) {
    if (!canConvert(rel, to)) {
      return null;
    }

    Sort sort = (Sort) traitDef.convert(null, rel, to, false);

    if (sort == null) {
      return null;
    }

    return CascadesTestSort.create(rel, to, null, null);
  }

  public static boolean canConvert(RelNode rel, RelCollation to) {
    // Returns true only if we can convert.  In this case, we can only convert
    // if the fromTrait (the input) has fields that the toTrait wants to sort.
    for (RelFieldCollation field : to.getFieldCollations()) {
      int index = field.getFieldIndex();
      if (index >= rel.getRowType().getFieldCount()) {
        return false;
      }
    }
    return true;
  }
}
