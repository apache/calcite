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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

/**
 * Class that enforces desired trait for {@link RelNode}.
 * TODO: Use implementation rule instead?
 * @param <T> Trait.
 */
public abstract class Enforcer<T extends RelTrait> {
  protected final RelTraitDef<T> traitDef;

  public Enforcer(RelTraitDef<T> traitDef) {
    this.traitDef = traitDef;
  }

  public abstract RelNode enforce(RelNode rel, T to);

  public RelTraitDef<T> traitDef() {
    return traitDef;
  }
}
