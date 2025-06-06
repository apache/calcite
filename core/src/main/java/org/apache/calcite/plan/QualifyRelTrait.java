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
package org.apache.calcite.plan;

/**
 * Qualify rel trait is used to identify if a given rel is a qualify rel or not.
 */
public class QualifyRelTrait implements RelTrait {
  private final String clauseName;

  public QualifyRelTrait(String clauseName) {
    this.clauseName = clauseName;
  }

  public final String getClauseName() {
    return this.clauseName;
  }

  public RelTraitDef<QualifyRelTrait> getTraitDef() {
    return QualifyRelTraitDef.instance;
  }

  public boolean satisfies(RelTrait trait) {
    throw new UnsupportedOperationException("Method not implemented for TableAliasTrait");
  }

  public void register(RelOptPlanner planner) {
    throw new UnsupportedOperationException("Registration not supported for TableAliasTrait");
  }
}
