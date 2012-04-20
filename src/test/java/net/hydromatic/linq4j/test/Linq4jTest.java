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
package net.hydromatic.linq4j.test;

import junit.framework.TestCase;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Extensions;
import net.hydromatic.linq4j.function.*;

import java.util.*;

/**
 * Tests for LINQ4J.
 */
public class Linq4jTest extends TestCase {
    public void testSelect() {
        List<String> names =
            Extensions.asEnumerable(emps)
                .select(new Function1<Employee, String>() {
                    public String apply(Employee employee) {
                        return employee.name;
                    }
                }).toList();
        assertEquals("[Fred, Bill, Eric, Jane]", names.toString());
    }

    public void testWhere() {
        List<String> names =
            Extensions.asEnumerable(emps)
                .where(
                    new Predicate1<Employee>() {
                        public boolean apply(Employee employee) {
                            return employee.deptno < 15;
                        }
                    })
                .select(
                    new Function1<Employee, String>() {
                        public String apply(Employee employee) {
                            return employee.name;
                        }
                    })
        .toList();
        assertEquals("[Fred, Eric, Jane]", names.toString());
    }

    public void testSelectMany() {
        final List<String> nameSeqs =
            Extensions.asEnumerable(depts)
                .selectMany(
                    new Function1<Department, Enumerable<Employee>>() {
                        public Enumerable<Employee> apply(Department a0) {
                            return Extensions.asEnumerable(a0.employees);
                        }
                    })
                .select(
                    new Function2<Employee, Integer, String>() {
                        public String apply(Employee arg0, Integer arg1) {
                            return "#" + arg1 + ": " + arg0.name;
                        }
                    }
                )
                .toList();
        assertEquals(
            "[#0: Fred, #1: Eric, #2: Jane, #3: Bill]", nameSeqs.toString());
    }

    public static class Employee {
        final int empno;
        final String name;
        final int deptno;

        public Employee(int empno, String name, int deptno) {
            this.empno = empno;
            this.name = name;
            this.deptno = deptno;
        }

        public String toString() {
            return "Employee(name: " + name + ", deptno:" + deptno + ")";
        }
    }

    public static class Department {
        final String name;
        final int deptno;
        final List<Employee> employees;

        public Department(String name, int deptno, List<Employee> employees) {
            this.name = name;
            this.deptno = deptno;
            this.employees = employees;
        }

        public String toString() {
            return "Department(name: " + name
                   + ", deptno:" + deptno
                   + ", employees: " + employees
                   + ")";
        }
    }

    public static final Employee[] emps = {
        new Employee(100, "Fred", 10),
        new Employee(110, "Bill", 30),
        new Employee(120, "Eric", 10),
        new Employee(130, "Jane", 10),
    };

    public static final Department[] depts = {
        new Department("Sales", 10, Arrays.asList(emps[0], emps[2], emps[3])),
        new Department("HR", 20, Collections.<Employee>emptyList()),
        new Department("Marketing", 30, Arrays.asList(emps[1])),
    };
}

// End Linq4jTest.java
