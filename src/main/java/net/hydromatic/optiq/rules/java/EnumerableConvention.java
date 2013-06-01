/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.rules.java;

import org.eigenbase.relopt.*;


/**
 * Family of calling conventions that return results as an
 * {@link net.hydromatic.linq4j.Enumerable}.
 */
public enum EnumerableConvention implements Convention {
  /** Convention that returns result as an Enumerable, each record as an array
   * of objects. Records of 0 and 1 fields are represented as empty lists
   * and scalars, respectively. */
  ARRAY(JavaRowFormat.ARRAY),

  /** Convention that returns result as an Enumerable, each record as an
   * object that has one data member for each column. Records of 0 and 1
   * fields are represented as empty lists and scalars, respectively. */
  CUSTOM(JavaRowFormat.CUSTOM);

  private final String name;
  public final JavaRowFormat format;

  EnumerableConvention(JavaRowFormat format) {
    this.format = format;
    this.name = "ENUMERABLE_" + name();
  }

  public Class getInterface() {
    return EnumerableRel.class;
  }

  public String getName() {
    return name;
  }

  public RelTraitDef getTraitDef() {
    return ConventionTraitDef.instance;
  }
}

// End EnumerableConvention.java
