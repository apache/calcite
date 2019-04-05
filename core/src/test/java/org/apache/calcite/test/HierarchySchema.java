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
package org.apache.calcite.test;

import java.util.Arrays;
import java.util.Objects;

/**
 * A Schema representing a hierarchy of employees.
 *
 * <p>The Schema is meant to be used with
 * {@link org.apache.calcite.adapter.java.ReflectiveSchema} thus all
 * fields, and methods, should be public.
 */
public class HierarchySchema {
  @Override public String toString() {
    return "HierarchySchema";
  }

  public final JdbcTest.Employee[] emps = {
      new JdbcTest.Employee(1, 10, "Emp1", 10000, 1000),
      new JdbcTest.Employee(2, 10, "Emp2", 8000, 500),
      new JdbcTest.Employee(3, 10, "Emp3", 7000, null),
      new JdbcTest.Employee(4, 10, "Emp4", 8000, 500),
      new JdbcTest.Employee(5, 10, "Emp5", 7000, null),
  };

  public final JdbcTest.Department[] depts = {
      new JdbcTest.Department(
          10,
          "Dept",
          Arrays.asList(emps[0], emps[1], emps[2], emps[3], emps[4]),
          new JdbcTest.Location(-122, 38)),
  };

  //      Emp1
  //      /  \
  //    Emp2  Emp4
  //    /  \
  // Emp3   Emp5
  public final Hierarchy[] hierarchies = {
      new Hierarchy(1, 2),
      new Hierarchy(2, 3),
      new Hierarchy(2, 5),
      new Hierarchy(1, 4),
  };

  /**
   * Hierarchy representing manager - subordinate
   */
  public static class Hierarchy {
    public final int managerid;
    public final int subordinateid;

    public Hierarchy(int managerid, int subordinateid) {
      this.managerid = managerid;
      this.subordinateid = subordinateid;
    }

    @Override public String toString() {
      return "Hierarchy [managerid: " + managerid + ", subordinateid: " + subordinateid + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Hierarchy
          && managerid == ((Hierarchy) obj).managerid
          && subordinateid == ((Hierarchy) obj).subordinateid;
    }

    @Override public int hashCode() {
      return Objects.hash(managerid, subordinateid);
    }
  }
}

// End HierarchySchema.java
