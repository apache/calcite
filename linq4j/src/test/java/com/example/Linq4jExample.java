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
package com.example;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;

/**
 * Example using linq4j to query in-memory collections.
 */
public class Linq4jExample {
  private Linq4jExample() {}

  /** Employee. */
  public static class Employee {
    public final int empno;
    public final String name;
    public final int deptno;

    public Employee(int empno, String name, int deptno) {
      this.empno = empno;
      this.name = name;
      this.deptno = deptno;
    }

    public String toString() {
      return "Employee(name: " + name + ", deptno:" + deptno + ")";
    }
  }

  public static final Employee[] EMPS = {
      new Employee(100, "Fred", 10),
      new Employee(110, "Bill", 30),
      new Employee(120, "Eric", 10),
      new Employee(130, "Janet", 10),
  };

  public static final Function1<Employee, Integer> EMP_DEPTNO_SELECTOR =
      employee -> employee.deptno;

  public static void main(String[] args) {
    String s = Linq4j.asEnumerable(EMPS)
        .groupBy(
            EMP_DEPTNO_SELECTOR,
            (Function0<String>) () -> null,
            (v1, e0) -> v1 == null ? e0.name : (v1 + "+" + e0.name),
            (v1, v2) -> v1 + ": " + v2)
        .orderBy(Functions.identitySelector())
        .toList()
        .toString();
    assert s.equals("[10: Fred+Eric+Janet, 30: Bill]");
  }
}

// End Linq4jExample.java
