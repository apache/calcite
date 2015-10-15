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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * HumanResources schema for tests.
 */
public class HrSchema {
  @Override public String toString() {
    return "HrSchema";
  }

  public final Employee[] emps = {
    new Employee(100, 10, "Bill", 10000, 1000),
    new Employee(200, 20, "Eric", 8000, 500),
    new Employee(150, 10, "Sebastian", 7000, null),
    new Employee(110, 10, "Theodore", 11500, 250),
  };
  public final Department[] depts = {
    new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
        new Location(-122, 38)),
    new Department(30, "Marketing", Collections.<Employee>emptyList(),
        new Location(0, 52)),
    new Department(40, "HR", Collections.singletonList(emps[1]), null),
  };
  public final Dependent[] dependents = {
    new Dependent(10, "Michael"),
    new Dependent(10, "Jane"),
  };
  public final Dependent[] locations = {
    new Dependent(10, "San Francisco"),
    new Dependent(20, "San Diego"),
  };

  public static TranslatableTable view(String s) {
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.INTEGER)
                .build();
          }
        }, "values (1), (3), " + s, ImmutableList.<String>of());
  }

  public static TranslatableTable str(Object o, Object p) {
    assertTrue(RexLiteral.validConstant(o, Litmus.THROW));
    assertTrue(RexLiteral.validConstant(p, Litmus.THROW));
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.VARCHAR, 100)
                .build();
          }
        },
        "values " + SqlDialect.CALCITE.quoteStringLiteral(o.toString())
            + ", " + SqlDialect.CALCITE.quoteStringLiteral(p.toString()),
        ImmutableList.<String>of());
  }

  /** An employee. */
  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }

    public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }
  }

  /** A store department. */
  public static class Department {
    public final int deptno;
    public final String name;

    @org.apache.calcite.adapter.java.Array(component = Employee.class)
    public final List<Employee> employees;
    public final Location location;

    public Department(int deptno, String name, List<Employee> employees,
        Location location) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
      this.location = location;
    }

    public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + ", location: " + location + "]";
    }
  }

  /** A two-dimensional point. */
  public static class Location {
    public final int x;
    public final int y;

    public Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override public String toString() {
      return "Location [x: " + x + ", y: " + y + "]";
    }
  }

  /** A struct relating employee and name. */
  public static class Dependent {
    public final int empid;
    public final String name;

    public Dependent(int empid, String name) {
      this.empid = empid;
      this.name = name;
    }

    @Override public String toString() {
      return "Dependent [empid: " + empid + ", name: " + name + "]";
    }

  }
}

// End HrSchema.java
