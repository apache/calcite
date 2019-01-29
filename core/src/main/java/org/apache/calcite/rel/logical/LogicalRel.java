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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ImplicitTrait;

import java.util.function.Supplier;

/**
 * Marker interface for Logical relations.
 * {@code @ImplicitTrait} helps planner to know rels that implement {@link LogicalRel} have
 * Logical convention so it knows the class is specific enough.
 */
@ImplicitTrait(LogicalRel.ConventionFactory.class)
public interface LogicalRel {
  /** Returns Convention.NONE. © Captain Obvious */
  class ConventionFactory implements Supplier<Convention> {
    @Override public Convention get() {
      return Convention.NONE;
    }
  }
}

// End LogicalRel.java
