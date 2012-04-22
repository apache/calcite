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
import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.function.*;

import java.util.*;

/**
 * Tests for LINQ4J.
 */
public class Linq4jTest extends TestCase {

    public static final Function1<Employee,String> EMP_NAME_SELECTOR =
        new Function1<Employee, String>() {
            public String apply(Employee employee) {
                return employee.name;
            }
        };

    public static final Function1<Employee, Integer> EMP_DEPTNO_SELECTOR =
        new Function1<Employee, Integer>() {
            public Integer apply(Employee employee) {
                return employee.deptno;
            }
        };

    public static final Function1<Employee, Integer> EMP_EMPNO_SELECTOR =
        new Function1<Employee, Integer>() {
            public Integer apply(Employee employee) {
                return employee.empno;
            }
        };

    public static final Function1<Department,Enumerable<Employee>> DEPT_EMPLOYEES_SELECTOR =
        new Function1<Department, Enumerable<Employee>>() {
            public Enumerable<Employee> apply(Department a0) {
                return Linq4j.asEnumerable(a0.employees);
            }
        };

    public void testSelect() {
        List<String> names =
            Linq4j.asEnumerable(emps)
                .select(EMP_NAME_SELECTOR)
                .toList();
        assertEquals("[Fred, Bill, Eric, Jane]", names.toString());
    }

    public void testWhere() {
        List<String> names =
            Linq4j.asEnumerable(emps)
                .where(
                    new Predicate1<Employee>() {
                        public boolean apply(Employee employee) {
                            return employee.deptno < 15;
                        }
                    })
                .select(EMP_NAME_SELECTOR)
                .toList();
        assertEquals("[Fred, Eric, Jane]", names.toString());
    }

    public void testWhereIndexed() {
        // Returns every other employee.
        List<String> names =
            Linq4j.asEnumerable(emps)
                .where(
                   new Predicate2<Employee, Integer>() {
                        public boolean apply(Employee employee, Integer n) {
                            return n % 2 == 0;
                        }
                    })
                .select(EMP_NAME_SELECTOR)
                .toList();
        assertEquals("[Fred, Eric]", names.toString());
    }

    public void testSelectMany() {
        final List<String> nameSeqs =
            Linq4j.asEnumerable(depts)
                .selectMany(DEPT_EMPLOYEES_SELECTOR)
                .select(
                    new Function2<Employee, Integer, String>() {
                        public String apply(Employee v1, Integer v2) {
                            return "#" + v2 + ": " + v1.name;
                        }
                    }
                )
                .toList();
        assertEquals(
            "[#0: Fred, #1: Eric, #2: Jane, #3: Bill]", nameSeqs.toString());
    }

    public void testCount() {
        final int count = Linq4j.asEnumerable(depts).count();
        assertEquals(3, count);
    }

    public void testCountPredicate() {
        final int count =
            Linq4j.asEnumerable(depts).count(
                new Predicate1<Department>() {
                    public boolean apply(Department v1) {
                        return v1.employees.size() > 0;
                    }
                });
        assertEquals(2, count);
    }

    public void testLongCount() {
        final long count = Linq4j.asEnumerable(depts).longCount();
        assertEquals(3, count);
    }

    public void testLongCountPredicate() {
        final long count =
            Linq4j.asEnumerable(depts).longCount(
                new Predicate1<Department>() {
                    public boolean apply(Department v1) {
                        return v1.employees.size() > 0;
                    }
                });
        assertEquals(2, count);
    }

    public void testToMap() {
        final Map<Integer, Employee> map =
            Linq4j.asEnumerable(emps)
                .toMap(EMP_EMPNO_SELECTOR);
        assertEquals(4, map.size());
        assertTrue(map.get(110).name.equals("Bill"));
    }

    public void testToMap2() {
        final Map<Integer, Integer> map =
            Linq4j.asEnumerable(emps)
                .toMap(EMP_EMPNO_SELECTOR, EMP_DEPTNO_SELECTOR);
        assertEquals(4, map.size());
        assertTrue(map.get(110) == 30);
    }

    public void testToLookup() {
        final Lookup<Integer, Employee> lookup =
            Linq4j.asEnumerable(emps).toLookup(
                EMP_DEPTNO_SELECTOR);
        int n = 0;
        for (Grouping<Integer, Employee> grouping : lookup) {
            ++n;
            switch (grouping.getKey()) {
            case 10:
                assertEquals(3, grouping.count());
                break;
            case 30:
                assertEquals(1, grouping.count());
                break;
            default:
                fail("unknown department number " + grouping);
            }
        }
        assertEquals(n, 2);
    }

    public void testToLookupSelector() {
        final Lookup<Integer, String> lookup =
            Linq4j.asEnumerable(emps).toLookup(
                EMP_DEPTNO_SELECTOR,
                EMP_NAME_SELECTOR);
        int n = 0;
        for (Grouping<Integer, String> grouping : lookup) {
            ++n;
            switch (grouping.getKey()) {
            case 10:
                assertEquals(3, grouping.count());
                assertTrue(grouping.contains("Fred"));
                assertTrue(grouping.contains("Eric"));
                assertTrue(grouping.contains("Jane"));
                assertFalse(grouping.contains("Bill"));
                break;
            case 30:
                assertEquals(1, grouping.count());
                assertTrue(grouping.contains("Bill"));
                assertFalse(grouping.contains("Fred"));
                break;
            default:
                fail("unknown department number " + grouping);
            }
        }
        assertEquals(n, 2);

        assertEquals(
            "[10:3, 30:1]",
            lookup.applyResultSelector(
                new Function2<Integer, Enumerable<String>, Object>() {
                    public Object apply(Integer v1, Enumerable<String> v2) {
                        return v1 + ":" + v2.count();
                    }
                }
            )
                .toList()
                .toString());
    }

    public void testCast() {
        final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
        final Enumerator<Integer> enumerator =
            Linq4j.asEnumerable(numbers)
                .cast(Integer.class)
                .enumerator();
        checkCast(enumerator);
    }

    public void testIterableCast() {
        final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
        final Enumerator<Integer> enumerator =
            Linq4j.cast(numbers, Integer.class)
                .enumerator();
        checkCast(enumerator);
    }

    private void checkCast(Enumerator<Integer> enumerator) {
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(2), enumerator.current());
        assertTrue(enumerator.moveNext());
        assertNull(enumerator.current());
        assertTrue(enumerator.moveNext());
        try {
            Object x = enumerator.current();
            fail("expected error, got " + x);
        } catch (ClassCastException e) {
            // good
        }
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(5), enumerator.current());
        assertFalse(enumerator.moveNext());
        enumerator.reset();
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(2), enumerator.current());
    }

    public void testOfType() {
        final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
        final Enumerator<Integer> enumerator =
            Linq4j.asEnumerable(numbers)
                .ofType(Integer.class)
                .enumerator();
        checkIterable(enumerator);
    }

    public void testIterableOfType() {
        final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
        final Enumerator<Integer> enumerator =
            Linq4j.ofType(numbers, Integer.class)
                .enumerator();
        checkIterable(enumerator);
    }

    private void checkIterable(Enumerator<Integer> enumerator) {
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(2), enumerator.current());
        assertTrue(enumerator.moveNext());
        assertNull(enumerator.current());
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(5), enumerator.current());
        assertFalse(enumerator.moveNext());
        enumerator.reset();
        assertTrue(enumerator.moveNext());
        assertEquals(Integer.valueOf(2), enumerator.current());
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
