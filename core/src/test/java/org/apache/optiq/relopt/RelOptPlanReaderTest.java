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
package org.apache.optiq.relopt;

import org.apache.optiq.rel.*;

import org.apache.optiq.impl.jdbc.JdbcRules;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link org.apache.optiq.rel.RelJson}.
 */
public class RelOptPlanReaderTest {
  @Test public void testTypeToClass() {
    RelJson relJson = new RelJson(null);

    // in org.apache.optiq.rel package
    assertThat(relJson.classToTypeName(ProjectRel.class),
        is("ProjectRel"));
    assertThat(relJson.typeNameToClass("ProjectRel"),
        sameInstance((Class) ProjectRel.class));

    // in org.apache.optiq.impl.jdbc.JdbcRules outer class
    assertThat(relJson.classToTypeName(JdbcRules.JdbcProjectRel.class),
        is("JdbcProjectRel"));
    assertThat(relJson.typeNameToClass("JdbcProjectRel"),
        equalTo((Class) JdbcRules.JdbcProjectRel.class));

    try {
      Class clazz = relJson.typeNameToClass("NonExistentRel");
      fail("expected exception, got " + clazz);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), is("unknown type NonExistentRel"));
    }
    try {
      Class clazz =
          relJson.typeNameToClass("org.apache.optiq.rel.NonExistentRel");
      fail("expected exception, got " + clazz);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          is("unknown type org.apache.optiq.rel.NonExistentRel"));
    }

    // In this class; no special treatment. Note: '$MyRel' not '.MyRel'.
    assertThat(relJson.classToTypeName(MyRel.class),
        is("org.apache.optiq.relopt.RelOptPlanReaderTest$MyRel"));
    assertThat(relJson.typeNameToClass(MyRel.class.getName()),
        equalTo((Class) MyRel.class));

    // Using canonical name (with '$'), not found
    try {
      Class clazz =
          relJson.typeNameToClass(MyRel.class.getCanonicalName());
      fail("expected exception, got " + clazz);
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), is("unknown type "
            + "org.apache.optiq.relopt.RelOptPlanReaderTest.MyRel"));
    }
  }

  public static class MyRel extends AbstractRelNode {
    public MyRel(RelOptCluster cluster, RelTraitSet traitSet) {
      super(cluster, traitSet);
    }
  }
}

// End RelOptPlanReaderTest.java
