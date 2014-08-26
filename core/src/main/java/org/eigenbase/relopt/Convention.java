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
package org.eigenbase.relopt;

import org.eigenbase.rel.RelNode;

/**
 * Calling convention trait.
 */
public interface Convention extends RelTrait {
  /**
   * Convention that for a relational expression that does not support any
   * convention. It is not implementable, and has to be transformed to
   * something else in order to be implemented.
   *
   * <p>Relational expressions generally start off in this form.</p>
   *
   * <p>Such expressions always have infinite cost.</p>
   */
  Convention NONE = new Impl("NONE", RelNode.class);

  Class getInterface();

  String getName();

  /**
   * Default implementation.
   */
  class Impl implements Convention {
    private final String name;
    private final Class<? extends RelNode> relClass;

    public Impl(String name, Class<? extends RelNode> relClass) {
      this.name = name;
      this.relClass = relClass;
    }

    @Override public String toString() {
      return getName();
    }

    public void register(RelOptPlanner planner) {}

    public boolean subsumes(RelTrait trait) {
      return this == trait;
    }

    public Class getInterface() {
      return relClass;
    }

    public String getName() {
      return name;
    }

    public RelTraitDef getTraitDef() {
      return ConventionTraitDef.INSTANCE;
    }
  }
}

// End Convention.java
