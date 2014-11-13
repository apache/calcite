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

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.*;

import com.example.Linq4jExample;

import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Tests for LINQ4J.
 */
public class Linq4jTest {
  public static final Function1<Employee, String> EMP_NAME_SELECTOR =
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

  public static final Function1<Department, Enumerable<Employee>>
  DEPT_EMPLOYEES_SELECTOR =
      new Function1<Department, Enumerable<Employee>>() {
        public Enumerable<Employee> apply(Department a0) {
          return Linq4j.asEnumerable(a0.employees);
        }
      };

  public static final Function1<Department, String> DEPT_NAME_SELECTOR =
      new Function1<Department, String>() {
        public String apply(Department department) {
          return department.name;
        }
      };

  public static final Function1<Department, Integer> DEPT_DEPTNO_SELECTOR =
      new Function1<Department, Integer>() {
        public Integer apply(Department department) {
          return department.deptno;
        }
      };

  public static final IntegerFunction1<Department> DEPT_DEPTNO_SELECTOR2 =
      new IntegerFunction1<Department>() {
        public int apply(Department department) {
          return department.deptno;
        }
      };

  public static final Function1<Object, Integer> ONE_SELECTOR =
      new Function1<Object, Integer>() {
        public Integer apply(Object employee) {
          return 1;
        }
      };

  private static final Function2<Object, Object, Integer> PAIR_SELECTOR =
      new Function2<Object, Object, Integer>() {
        public Integer apply(Object employee, Object v2) {
          return 1;
        }
      };

  @Test public void testSelect() {
    List<String> names =
        Linq4j.asEnumerable(emps)
            .select(EMP_NAME_SELECTOR)
            .toList();
    assertEquals("[Fred, Bill, Eric, Janet]", names.toString());
  }

  @Test public void testWhere() {
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
    assertEquals("[Fred, Eric, Janet]", names.toString());
  }

  @Test public void testWhereIndexed() {
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

  @Test public void testSelectMany() {
    final List<String> nameSeqs =
        Linq4j.asEnumerable(depts)
            .selectMany(DEPT_EMPLOYEES_SELECTOR)
            .select(
                new Function2<Employee, Integer, String>() {
                  public String apply(Employee v1, Integer v2) {
                    return "#" + v2 + ": " + v1.name;
                  }
                })
            .toList();
    assertEquals(
        "[#0: Fred, #1: Eric, #2: Janet, #3: Bill]", nameSeqs.toString());
  }

  @Test public void testCount() {
    final int count = Linq4j.asEnumerable(depts).count();
    assertEquals(3, count);
  }

  @Test public void testCountPredicate() {
    final int count =
        Linq4j.asEnumerable(depts).count(
            new Predicate1<Department>() {
              public boolean apply(Department v1) {
                return v1.employees.size() > 0;
              }
            });
    assertEquals(2, count);
  }

  @Test public void testLongCount() {
    final long count = Linq4j.asEnumerable(depts).longCount();
    assertEquals(3, count);
  }

  @Test public void testLongCountPredicate() {
    final long count =
        Linq4j.asEnumerable(depts).longCount(
            new Predicate1<Department>() {
              public boolean apply(Department v1) {
                return v1.employees.size() > 0;
              }
            });
    assertEquals(2, count);
  }

  @Test public void testAllPredicate() {
    Predicate1<Employee> allEmpnoGE100 = new Predicate1<Employee>() {
      public boolean apply(Employee emp) {
        return emp.empno >= 100;
      }
    };

    Predicate1<Employee> allEmpnoGT100 = new Predicate1<Employee>() {
      public boolean apply(Employee emp) {
        return emp.empno > 100;
      }
    };

    assertTrue(Linq4j.asEnumerable(emps).all(allEmpnoGE100));
    assertFalse(Linq4j.asEnumerable(emps).all(allEmpnoGT100));
  }

  @Test public void testAny() {
    List<Employee> emptyList = Collections.<Employee>emptyList();
    assertFalse(Linq4j.asEnumerable(emptyList).any());
    assertTrue(Linq4j.asEnumerable(emps).any());
  }

  @Test public void testAnyPredicate() {
    Predicate1<Department> deptoNameIT = new Predicate1<Department>() {
      public boolean apply(Department v1) {
        return v1.name != null && v1.name.equals("IT");
      }
    };

    Predicate1<Department> deptoNameSales = new Predicate1<Department>() {
      public boolean apply(Department v1) {
        return v1.name != null && v1.name.equals("Sales");
      }
    };

    assertFalse(Linq4j.asEnumerable(depts).any(deptoNameIT));
    assertTrue(Linq4j.asEnumerable(depts).any(deptoNameSales));
  }

  @Test public void testAverageSelector() {
    assertEquals(
        20,
        Linq4j.asEnumerable(depts).average(DEPT_DEPTNO_SELECTOR2));
  }

  @Test public void testMin() {
    assertEquals(
        10,
        (int) Linq4j.asEnumerable(depts).select(DEPT_DEPTNO_SELECTOR)
            .min());
  }

  @Test public void testMinSelector() {
    assertEquals(
        10,
        (int) Linq4j.asEnumerable(depts).min(DEPT_DEPTNO_SELECTOR));
  }

  @Test public void testMinSelector2() {
    assertEquals(
        10,
        Linq4j.asEnumerable(depts).min(DEPT_DEPTNO_SELECTOR2));
  }

  @Test public void testMax() {
    assertEquals(
        30,
        (int) Linq4j.asEnumerable(depts).select(DEPT_DEPTNO_SELECTOR)
            .max());
  }

  @Test public void testMaxSelector() {
    assertEquals(
        30,
        (int) Linq4j.asEnumerable(depts).max(DEPT_DEPTNO_SELECTOR));
  }

  @Test public void testMaxSelector2() {
    assertEquals(
        30,
        Linq4j.asEnumerable(depts).max(DEPT_DEPTNO_SELECTOR2));
  }

  @Test public void testAggregate() {
    assertEquals(
        "Sales,HR,Marketing",
        Linq4j.asEnumerable(depts)
            .select(DEPT_NAME_SELECTOR)
            .aggregate(
                null,
                new Function2<String, String, String>() {
                  public String apply(String v1, String v2) {
                    return v1 == null ? v2 : v1 + "," + v2;
                  }
                }));
  }

  @Test public void testToMap() {
    final Map<Integer, Employee> map =
        Linq4j.asEnumerable(emps)
            .toMap(EMP_EMPNO_SELECTOR);
    assertEquals(4, map.size());
    assertTrue(map.get(110).name.equals("Bill"));
  }

  @Test public void testToMap2() {
    final Map<Integer, Integer> map =
        Linq4j.asEnumerable(emps)
            .toMap(EMP_EMPNO_SELECTOR, EMP_DEPTNO_SELECTOR);
    assertEquals(4, map.size());
    assertTrue(map.get(110) == 30);
  }

  @Test public void testToLookup() {
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

  @Test public void testToLookupSelector() {
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
        assertTrue(grouping.contains("Janet"));
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
            new Function2<Integer, Enumerable<String>, String>() {
              public String apply(Integer v1, Enumerable<String> v2) {
                return v1 + ":" + v2.count();
              }
            })
            .orderBy(Functions.<String>identitySelector())
            .toList().toString());
  }

  @Test public void testContains() {
    Employee e = emps[1];
    Employee employeeClone = new Employee(e.empno, e.name, e.deptno);
    Employee employeeOther = badEmps[0];

    assertEquals(e, employeeClone);
    assertTrue(Linq4j.asEnumerable(emps).contains(e));
    assertTrue(Linq4j.asEnumerable(emps).contains(employeeClone));
    assertFalse(Linq4j.asEnumerable(emps).contains(employeeOther));

  }

  @Test public void testContainsWithEqualityComparer() {
    EqualityComparer<Employee> compareByEmpno =
            new EqualityComparer<Employee>() {
        public boolean equal(Employee e1, Employee e2) {
          return e1 != null && e2 != null
                  && e1.empno == e2.empno;
        }

        public int hashCode(Employee t) {
          return t == null ? 0x789d : t.hashCode();
        }
      };

    Employee e = emps[1];
    Employee employeeClone = new Employee(e.empno, e.name, e.deptno);
    Employee employeeOther = badEmps[0];

    assertEquals(e, employeeClone);
    assertTrue(Linq4j.asEnumerable(emps)
            .contains(e, compareByEmpno));
    assertTrue(Linq4j.asEnumerable(emps)
            .contains(employeeClone, compareByEmpno));
    assertFalse(Linq4j.asEnumerable(emps)
            .contains(employeeOther, compareByEmpno));

  }

  @Test public void testFirst() {
    Employee e = emps[0];
    assertEquals(e, emps[0]);
    assertEquals(e, Linq4j.asEnumerable(emps).first());

    Department d = depts[0];
    assertEquals(d, depts[0]);
    assertEquals(d, Linq4j.asEnumerable(depts).first());

    try {
      String s = Linq4j.<String>emptyEnumerable().first();
      fail("expected exception, got " + s);
    } catch (NoSuchElementException ex) {
      // ok
    }

    // close occurs if first throws
    final int[] closeCount = {0};
    try {
      String s = myEnumerable(closeCount, 0).first();
      fail("expected exception, got " + s);
    } catch (NoSuchElementException ex) {
      // ok
    }
    assertThat(closeCount[0], equalTo(1));

    // close occurs if first does not throw
    closeCount[0] = 0;
    final String s = myEnumerable(closeCount, 1).first();
    assertThat(s, equalTo("x"));
    assertThat(closeCount[0], equalTo(1));
  }

  private Enumerable<String> myEnumerable(final int[] closes, final int size) {
    return new AbstractEnumerable<String>() {
      public Enumerator<String> enumerator() {
        return new Enumerator<String>() {
          int i = 0;

          public String current() {
            return "x";
          }

          public boolean moveNext() {
            return i++ < size;
          }

          public void reset() {
          }

          public void close() {
            ++closes[0];
          }
        };
      }
    };
  }

  @Test public void testFirstPredicate1() {
    Predicate1<String> startWithS = new Predicate1<String>() {
      public boolean apply(String s) {
        return s != null && Character.toString(s.charAt(0)).equals("S");
      }
    };

    Predicate1<Integer> numberGT15 = new Predicate1<Integer>() {
      public boolean apply(Integer i) {
        return i > 15;
      }
    };

    String[] people = {"Brill", "Smith", "Simpsom"};
    String[] peopleWithoutCharS = {"Brill", "Andrew", "Alice"};
    Integer[] numbers = {5, 10, 15, 20, 25};

    assertEquals(people[1], Linq4j.asEnumerable(people).first(startWithS));
    assertEquals(numbers[3], Linq4j.asEnumerable(numbers).first(numberGT15));

    // FIXME: What we need return if no one element is satisfied?
    assertNull(Linq4j.asEnumerable(peopleWithoutCharS).first(startWithS));

  }

  @SuppressWarnings("UnnecessaryBoxing")
  @Test public void testIdentityEqualityComparer() {
    final Integer one = new Integer(1);
    final Integer one2 = new Integer(1);
    final Integer two = new Integer(2);
    final EqualityComparer<Integer> idComparer = Functions.identityComparer();
    assertTrue(idComparer.equal(one, one));
    assertTrue(idComparer.equal(one, one2));
    assertFalse(idComparer.equal(one, two));
  }

  @Test public void testSelectorEqualityComparer() {
    final EqualityComparer<Employee> comparer =
        Functions.selectorComparer(
            new Function1<Employee, Object>() {
              public Object apply(Employee a0) {
                return a0.deptno;
              }
            });
    assertTrue(comparer.equal(emps[0], emps[0]));
    assertEquals(comparer.hashCode(emps[0]), comparer.hashCode(emps[0]));

    assertTrue(comparer.equal(emps[0], emps[2]));
    assertEquals(comparer.hashCode(emps[0]), comparer.hashCode(emps[2]));

    assertFalse(comparer.equal(emps[0], emps[1]));
    // not 100% guaranteed, but works for this data
    assertNotEquals(comparer.hashCode(emps[0]), comparer.hashCode(emps[1]));

    assertFalse(comparer.equal(emps[0], null));
    assertNotEquals(comparer.hashCode(emps[0]), comparer.hashCode(null));

    assertFalse(comparer.equal(null, emps[1]));
    assertTrue(comparer.equal(null, null));
    assertEquals(comparer.hashCode(null), comparer.hashCode(null));
  }

  @Test public void testToLookupSelectorComparer() {
    final Lookup<String, Employee> lookup =
        Linq4j.asEnumerable(emps).toLookup(
            EMP_NAME_SELECTOR,
            new EqualityComparer<String>() {
              public boolean equal(String v1, String v2) {
                return v1.length() == v2.length();
              }

              public int hashCode(String s) {
                return s.length();
              }
            });
    assertEquals(2, lookup.size());
    assertEquals(
        "[Fred, Janet]",
        new TreeSet<String>(lookup.keySet()).toString());

    StringBuilder buf = new StringBuilder();
    for (Grouping<String, Employee> grouping
        : lookup.orderBy(Linq4jTest.<String, Employee>groupingKeyExtractor())) {
      buf.append(grouping).append("\n");
    }
    assertEquals(
        "Fred: [Employee(name: Fred, deptno:10), Employee(name: Bill, deptno:30), Employee(name: Eric, deptno:10)]\n"
        + "Janet: [Employee(name: Janet, deptno:10)]\n",
        buf.toString());
  }

  private static <K extends Comparable, V> Function1<Grouping<K, V>, K>
  groupingKeyExtractor() {
    return new Function1<Grouping<K, V>, K>() {
      public K apply(Grouping<K, V> a0) {
        return a0.getKey();
      }
    };
  }

  /**
   * Tests the version of {@link ExtendedEnumerable#groupBy}
   * that uses an accumulator; does not build intermediate lists.
   */
  @Test public void testGroupBy() {
    String s =
        Linq4j.asEnumerable(emps)
            .groupBy(
                EMP_DEPTNO_SELECTOR, new Function0<String>() {
                  public String apply() {
                    return null;
                  }
                }, new Function2<String, Employee, String>() {
                  public String apply(String v1, Employee e0) {
                    return v1 == null ? e0.name : (v1 + "+" + e0.name);
                  }
                }, new Function2<Integer, String, String>() {
                  public String apply(Integer v1, String v2) {
                    return v1 + ": " + v2;
                  }
                })
            .orderBy(Functions.<String>identitySelector())
            .toList()
            .toString();
    assertEquals(
        "[10: Fred+Eric+Janet, 30: Bill]",
        s);
  }

  /**
   * Tests the version of
   * {@link ExtendedEnumerable#aggregate}
   * that has a result selector. Note how similar it is to
   * {@link #testGroupBy()}.
   */
  @Test public void testAggregate2() {
    String s =
        Linq4j.asEnumerable(emps)
            .aggregate(
                new Function0<String>() {
                  public String apply() {
                    return null;
                  }
                }.apply(), //CHECKSTYLE: IGNORE 0
                new Function2<String, Employee, String>() {
                  public String apply(String v1, Employee e0) {
                    return v1 == null ? e0.name : (v1 + "+" + e0.name);
                  }
                },
                new Function1<String, String>() {
                  public String apply(String v2) {
                    return "<no key>: " + v2;
                  }
                })
            .toString();
    assertEquals(
        "<no key>: Fred+Bill+Eric+Janet",
        s);
  }

  @Test public void testEmptyEnumerable() {
    final Enumerable<Object> enumerable = Linq4j.emptyEnumerable();
    assertThat(enumerable.any(), is(false));
    assertThat(enumerable.longCount(), equalTo(0L));
    final Enumerator<Object> enumerator = enumerable.enumerator();
    assertThat(enumerator.moveNext(), is(false));
  }

  @Test public void testSingletonEnumerable() {
    final Enumerable<String> enumerable = Linq4j.singletonEnumerable("foo");
    assertThat(enumerable.any(), is(true));
    assertThat(enumerable.longCount(), equalTo(1L));
    final Enumerator<String> enumerator = enumerable.enumerator();
    assertThat(enumerator.moveNext(), is(true));
    assertThat(enumerator.current(), equalTo("foo"));
    assertThat(enumerator.moveNext(), is(false));
  }

  @Test public void testSingletonEnumerator() {
    final Enumerator<String> enumerator = Linq4j.singletonEnumerator("foo");
    assertThat(enumerator.moveNext(), is(true));
    assertThat(enumerator.current(), equalTo("foo"));
    assertThat(enumerator.moveNext(), is(false));
  }

  @Test public void testSingletonNullEnumerator() {
    final Enumerator<String> enumerator = Linq4j.singletonNullEnumerator();
    assertThat(enumerator.moveNext(), is(true));
    assertThat(enumerator.current(), nullValue());
    assertThat(enumerator.moveNext(), is(false));
  }

  @Test public void testCast() {
    final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
    final Enumerator<Integer> enumerator =
        Linq4j.asEnumerable(numbers)
            .cast(Integer.class)
            .enumerator();
    checkCast(enumerator);
  }

  @Test public void testIterableCast() {
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

  @Test public void testOfType() {
    final List<Number> numbers = Arrays.asList((Number) 2, null, 3.14, 5);
    final Enumerator<Integer> enumerator =
        Linq4j.asEnumerable(numbers)
            .ofType(Integer.class)
            .enumerator();
    checkIterable(enumerator);
  }

  @Test public void testIterableOfType() {
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

  @Test public void testConcat() {
    assertEquals(
        5,
        Linq4j.asEnumerable(emps)
            .concat(Linq4j.asEnumerable(badEmps))
            .count());
  }

  @Test public void testUnion() {
    assertEquals(
        5,
        Linq4j.asEnumerable(emps)
            .union(Linq4j.asEnumerable(badEmps))
            .union(Linq4j.asEnumerable(emps))
            .count());
  }

  @Test public void testIntersect() {
    final Employee[] emps2 = {
      new Employee(150, "Theodore", 10),
      emps[3],
    };
    assertEquals(
        1,
        Linq4j.asEnumerable(emps)
            .intersect(Linq4j.asEnumerable(emps2))
            .count());
  }

  @Test public void testExcept() {
    final Employee[] emps2 = {
      new Employee(150, "Theodore", 10),
      emps[3],
    };
    assertEquals(
        3,
        Linq4j.asEnumerable(emps)
            .except(Linq4j.asEnumerable(emps2))
            .count());
  }

  @Test public void testDistinct() {
    final Employee[] emps2 = {
      new Employee(150, "Theodore", 10),
      emps[3],
      emps[0],
      emps[3],
    };
    assertEquals(
        3,
        Linq4j.asEnumerable(emps2)
            .distinct()
            .count());
  }

  @Test public void testDistinctWithEqualityComparer() {
    final Employee[] emps2 = {
      new Employee(150, "Theodore", 10),
      emps[3],
      emps[1],
      emps[3],
    };
    assertEquals(
        2,
        Linq4j.asEnumerable(emps2)
            .distinct(
                new EqualityComparer<Employee>() {
                  public boolean equal(Employee v1, Employee v2) {
                    return v1.deptno == v2.deptno;
                  }

                  public int hashCode(Employee employee) {
                    return employee.deptno;
                  }
                })
            .count());
  }

  @Test public void testGroupJoin() {
    // Note #1: Group join is a "left join": "bad employees" are filtered
    //   out, but empty departments are not.
    // Note #2: Order of departments is preserved.
    String s =
        Linq4j.asEnumerable(depts)
            .groupJoin(
                Linq4j.asEnumerable(emps)
                    .concat(Linq4j.asEnumerable(badEmps)),
                DEPT_DEPTNO_SELECTOR,
                EMP_DEPTNO_SELECTOR,
                new Function2<Department, Enumerable<Employee>, String>() {
                  public String apply(Department v1, Enumerable<Employee> v2) {
                    final StringBuilder buf = new StringBuilder("[");
                    int n = 0;
                    for (Employee employee : v2) {
                      if (n++ > 0) {
                        buf.append(", ");
                      }
                      buf.append(employee.name);
                    }
                    return buf.append("] work(s) in ").append(v1.name)
                        .toString();
                  }
                })
            .toList()
            .toString();
    assertEquals(
        "[[Fred, Eric, Janet] work(s) in Sales, "
        + "[] work(s) in HR, "
        + "[Bill] work(s) in Marketing]",
        s);
  }

  @Test public void testJoin() {
    // Note #1: Inner on both sides. Employees with bad departments,
    //   and departments with no employees are eliminated.
    // Note #2: Order of employees is preserved.
    String s =
        Linq4j.asEnumerable(emps)
            .concat(Linq4j.asEnumerable(badEmps))
            .join(
                Linq4j.asEnumerable(depts),
                EMP_DEPTNO_SELECTOR,
                DEPT_DEPTNO_SELECTOR,
                new Function2<Employee, Department, String>() {
                  public String apply(Employee v1, Department v2) {
                    return v1.name + " works in " + v2.name;
                  }
                })
            .orderBy(Functions.<String>identitySelector())
            .toList()
            .toString();
    assertEquals(
        "[Bill works in Marketing, "
        + "Eric works in Sales, "
        + "Fred works in Sales, "
        + "Janet works in Sales]",
        s);
  }

  @Test public void testLeftJoin() {
    // Note #1: Left join means emit nulls on RHS but not LHS.
    //   Employees with bad departments are not eliminated;
    //   departments with no employees are eliminated.
    // Note #2: Order of employees is preserved.
    String s =
        Linq4j.asEnumerable(emps)
            .concat(Linq4j.asEnumerable(badEmps))
            .join(
                Linq4j.asEnumerable(depts),
                EMP_DEPTNO_SELECTOR,
                DEPT_DEPTNO_SELECTOR,
                new Function2<Employee, Department, String>() {
                  public String apply(Employee v1, Department v2) {
                    return v1.name + " works in "
                        + (v2 == null ? null : v2.name);
                  }
                }, null, false, true)
            .orderBy(Functions.<String>identitySelector())
            .toList()
            .toString();
    assertEquals(
        "[Bill works in Marketing, "
        + "Cedric works in null, "
        + "Eric works in Sales, "
        + "Fred works in Sales, "
        + "Janet works in Sales]",
        s);
  }

  @Test public void testRightJoin() {
    // Note #1: Left join means emit nulls on LHS but not RHS.
    //   Employees with bad departments are eliminated;
    //   departments with no employees are not eliminated.
    // Note #2: Order of employees is preserved.
    String s =
        Linq4j.asEnumerable(emps)
            .concat(Linq4j.asEnumerable(badEmps))
            .join(
                Linq4j.asEnumerable(depts),
                EMP_DEPTNO_SELECTOR,
                DEPT_DEPTNO_SELECTOR,
                new Function2<Employee, Department, String>() {
                  public String apply(Employee v1, Department v2) {
                    return (v1 == null ? null : v1.name)
                        + " works in " + (v2 == null ? null : v2.name);
                  }
                }, null, true, false)
            .orderBy(Functions.<String>identitySelector())
            .toList()
            .toString();
    assertEquals(
        "[Bill works in Marketing, "
        + "Eric works in Sales, "
        + "Fred works in Sales, "
        + "Janet works in Sales, "
        + "null works in HR]",
        s);
  }

  @Test public void testFullJoin() {
    // Note #1: Full join means emit nulls both LHS and RHS.
    //   Employees with bad departments are not eliminated;
    //   departments with no employees are not eliminated.
    // Note #2: Order of employees is preserved.
    String s =
        Linq4j.asEnumerable(emps)
            .concat(Linq4j.asEnumerable(badEmps))
            .join(
                Linq4j.asEnumerable(depts),
                EMP_DEPTNO_SELECTOR,
                DEPT_DEPTNO_SELECTOR,
                new Function2<Employee, Department, String>() {
                  public String apply(Employee v1, Department v2) {
                    return (v1 == null ? null : v1.name)
                        + " works in " + (v2 == null ? null : v2.name);
                  }
                }, null, true, true)
            .orderBy(Functions.<String>identitySelector())
            .toList()
            .toString();
    assertEquals(
        "[Bill works in Marketing, "
        + "Cedric works in null, "
        + "Eric works in Sales, "
        + "Fred works in Sales, "
        + "Janet works in Sales, "
        + "null works in HR]",
        s);
  }

  @Test public void testJoinCartesianProduct() {
    int n =
        Linq4j.asEnumerable(emps)
            .<Department, Integer, Integer>join(
                Linq4j.asEnumerable(depts),
                (Function1) ONE_SELECTOR,
                (Function1) ONE_SELECTOR,
                (Function2) PAIR_SELECTOR)
            .count();
    assertEquals(12, n); // 4 employees times 3 departments
  }

  @SuppressWarnings("unchecked")
  @Test public void testCartesianProductEnumerator() {
    final Enumerable<String> abc =
        Linq4j.asEnumerable(Arrays.asList("a", "b", "c"));
    final Enumerable<String> xy =
        Linq4j.asEnumerable(Arrays.asList("x", "y"));

    final Enumerator<List<String>> product0 =
        Linq4j.product(
            Arrays.asList(Linq4j.<String>emptyEnumerator()));
    assertFalse(product0.moveNext());

    final Enumerator<List<String>> productFullEmpty =
        Linq4j.product(
            Arrays.asList(
                abc.enumerator(), Linq4j.<String>emptyEnumerator()));
    assertFalse(productFullEmpty.moveNext());

    final Enumerator<List<String>> productEmptyFull =
        Linq4j.product(
            Arrays.asList(
                abc.enumerator(), Linq4j.<String>emptyEnumerator()));
    assertFalse(productEmptyFull.moveNext());

    final Enumerator<List<String>> productAbcXy =
        Linq4j.product(
            Arrays.asList(abc.enumerator(), xy.enumerator()));
    assertTrue(productAbcXy.moveNext());
    assertEquals(Arrays.asList("a", "x"), productAbcXy.current());
    assertTrue(productAbcXy.moveNext());
    assertEquals(Arrays.asList("a", "y"), productAbcXy.current());
    assertTrue(productAbcXy.moveNext());
    assertEquals(Arrays.asList("b", "x"), productAbcXy.current());
    assertTrue(productAbcXy.moveNext());
    assertTrue(productAbcXy.moveNext());
    assertTrue(productAbcXy.moveNext());
    assertFalse(productAbcXy.moveNext());
  }

  @Test public void testAsQueryable() {
    // "count" is an Enumerable method.
    final int n =
        Linq4j.asEnumerable(emps)
            .asQueryable()
            .count();
    assertEquals(4, n);

    // "where" is a Queryable method
    // first, use a lambda
    ParameterExpression parameter =
        Expressions.parameter(Employee.class);
    final Queryable<Employee> nh =
        Linq4j.asEnumerable(emps)
            .asQueryable()
            .where(
                Expressions.lambda(
                    Predicate1.class,
                    Expressions.equal(
                        Expressions.field(
                            parameter,
                            Employee.class,
                            "deptno"),
                        Expressions.constant(10)),
                    parameter));
    assertEquals(3, nh.count());

    // second, use an expression
    final Queryable<Employee> nh2 =
        Linq4j.asEnumerable(emps)
            .asQueryable()
            .where(
                Expressions.lambda(
                    new Predicate1<Employee>() {
                      public boolean apply(Employee v1) {
                        return v1.deptno == 10;
                      }
                    }));
    assertEquals(3, nh2.count());

    // use lambda, this time call whereN
    ParameterExpression parameterE =
        Expressions.parameter(Employee.class);
    ParameterExpression parameterN =
        Expressions.parameter(Integer.TYPE);
    final Queryable<Employee> nh3 =
        Linq4j.asEnumerable(emps)
            .asQueryable()
            .whereN(
                Expressions.lambda(
                    Predicate2.class,
                    Expressions.andAlso(
                        Expressions.equal(
                            Expressions.field(
                                parameterE,
                                Employee.class,
                                "deptno"),
                            Expressions.constant(10)),
                        Expressions.lessThan(
                            parameterN,
                            Expressions.constant(3))),
                    parameterE,
                    parameterN));
    assertEquals(2, nh3.count());
  }

  @Test public void testTake() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> enumerableDeptsResult =
        enumerableDepts.take(2).toList();
    assertEquals(2, enumerableDeptsResult.size());
    assertEquals(depts[0], enumerableDeptsResult.get(0));
    assertEquals(depts[1], enumerableDeptsResult.get(1));

    final List<Department> enumerableDeptsResult5 =
        enumerableDepts.take(5).toList();
    assertEquals(3, enumerableDeptsResult5.size());
  }

  @Test public void testTake_enumerable() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> enumerableDeptsResult =
        EnumerableDefaults.take(enumerableDepts, 2).toList();
    assertEquals(2, enumerableDeptsResult.size());
    assertEquals(depts[0], enumerableDeptsResult.get(0));
    assertEquals(depts[1], enumerableDeptsResult.get(1));

    final List<Department> enumerableDeptsResult5 =
        EnumerableDefaults.take(enumerableDepts, 5).toList();
    assertEquals(3, enumerableDeptsResult5.size());
  }

  @Test public void testTake_queryable() {
    final Queryable<Department> querableDepts =
        Linq4j.asEnumerable(depts).asQueryable();
    final List<Department> queryableResult =
        QueryableDefaults.take(querableDepts, 2).toList();

    assertEquals(2, queryableResult.size());
    assertEquals(depts[0], queryableResult.get(0));
    assertEquals(depts[1], queryableResult.get(1));
  }

  @Test public void testTake_enumerable_zero_or_negative_size() {
    assertEquals(
        0,
        EnumerableDefaults.take(Linq4j.asEnumerable(depts), 0)
            .toList().size());
    assertEquals(
        0,
        EnumerableDefaults.take(Linq4j.asEnumerable(depts), -2)
            .toList().size());
  }

  @Test public void testTake_queryable_zero_or_negative_size() {
    assertEquals(
        0,
        QueryableDefaults.take(Linq4j.asEnumerable(depts).asQueryable(), 0)
            .toList().size());
    assertEquals(
        0,
        QueryableDefaults.take(Linq4j.asEnumerable(depts).asQueryable(), -2)
            .toList().size());
  }

  @Test public void testTake_enumerable_greater_than_length() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> depList =
        EnumerableDefaults.take(enumerableDepts, 5).toList();
    assertEquals(3, depList.size());
    assertEquals(depts[0], depList.get(0));
    assertEquals(depts[1], depList.get(1));
    assertEquals(depts[2], depList.get(2));
  }

  @Test public void testTake_queryable_greater_than_length() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> depList =
        EnumerableDefaults.take(enumerableDepts, 5).toList();
    assertEquals(3, depList.size());
    assertEquals(depts[0], depList.get(0));
    assertEquals(depts[1], depList.get(1));
    assertEquals(depts[2], depList.get(2));
  }

  @Test public void testTakeWhile_enumerable_predicate() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> deptList =
        EnumerableDefaults.takeWhile(
            enumerableDepts,
            new Predicate1<Department>() {
              public boolean apply(Department v1) {
                return v1.name.contains("e");
              }
            }).toList();

    // Only one department:
    // 0: Sales --> true
    // 1: HR --> false
    // 2: Marketing --> never get to it (we stop after false)
    assertEquals(1, deptList.size());
    assertEquals(depts[0], deptList.get(0));
  }

  @Test public void testTakeWhile_enumerable_function() {
    final Enumerable<Department> enumerableDepts =
        Linq4j.asEnumerable(depts);
    final List<Department> deptList =
        EnumerableDefaults.takeWhile(
            enumerableDepts,
            new Predicate2<Department, Integer>() {
              int index = 0;

              public boolean apply(Department v1, Integer v2) {
                // Make sure we're passed the correct indices
                assertEquals(
                  "Invalid index passed to function", index++, (int) v2);
                return 20 != v1.deptno;
              }
            }).toList();

    assertEquals(1, deptList.size());
    assertEquals(depts[0], deptList.get(0));
  }

  @Test public void testTakeWhile_queryable_functionexpression_predicate() {
    final Queryable<Department> queryableDepts =
        Linq4j.asEnumerable(depts).asQueryable();
    Predicate1<Department> predicate = new Predicate1<Department>() {
      public boolean apply(Department v1) {
        return "HR".equals(v1.name);
      }
    };
    List<Department> deptList =
        QueryableDefaults.takeWhile(
            queryableDepts, Expressions.lambda(predicate))
            .toList();

    assertEquals(0, deptList.size());

    predicate = new Predicate1<Department>() {
      public boolean apply(Department v1) {
        return "Sales".equals(v1.name);
      }
    };
    deptList =
        QueryableDefaults.takeWhile(
            queryableDepts, Expressions.lambda(predicate))
            .toList();

    assertEquals(1, deptList.size());
    assertEquals(depts[0], deptList.get(0));
  }

  @Test public void testTakeWhileN() {
    final Queryable<Department> queryableDepts =
        Linq4j.asEnumerable(depts).asQueryable();
    Predicate2<Department, Integer> function2 =
        new Predicate2<Department, Integer>() {
          int index = 0;
          public boolean apply(Department v1, Integer v2) {
            // Make sure we're passed the correct indices
            assertEquals(
                "Invalid index passed to function", index++, (int) v2);
            return v2 < 2;
          }
        };

    final List<Department> deptList =
        QueryableDefaults.takeWhileN(
            queryableDepts, Expressions.lambda(function2))
            .toList();

    assertEquals(2, deptList.size());
    assertEquals(depts[0], deptList.get(0));
    assertEquals(depts[1], deptList.get(1));
  }

  @Test public void testTakeWhileN_no_match() {
    final Queryable<Department> queryableDepts =
        Linq4j.asEnumerable(depts).asQueryable();
    Predicate2<Department, Integer> function2 = Functions.falsePredicate2();
    final List<Department> deptList =
        QueryableDefaults.takeWhileN(
            queryableDepts,
            Expressions.lambda(function2))
            .toList();

    assertEquals(0, deptList.size());
  }

  @Test public void testSkip() {
    assertEquals(2, Linq4j.asEnumerable(depts).skip(1).count());
    assertEquals(
        2,
        Linq4j.asEnumerable(depts).skipWhile(
            new Predicate1<Department>() {
              public boolean apply(Department v1) {
                return v1.name.equals("Sales");
              }
            }).count());
    assertEquals(
        3,
        Linq4j.asEnumerable(depts).skipWhile(
            new Predicate1<Department>() {
              public boolean apply(Department v1) {
                return !v1.name.equals("Sales");
              }
            }).count());
    assertEquals(
        1,
        Linq4j.asEnumerable(depts).skipWhile(
            new Predicate2<Department, Integer>() {
              public boolean apply(Department v1, Integer v2) {
                return v1.name.equals("Sales")
                       || v2 == 1;
              }
            }).count());

    assertEquals(
        2, Linq4j.asEnumerable(depts).skip(1).count());
    assertEquals(
        0, Linq4j.asEnumerable(depts).skip(5).count());
    assertEquals(
        1,
        Linq4j.asEnumerable(depts).skipWhile(
            new Predicate2<Department, Integer>() {
              public boolean apply(Department v1, Integer v2) {
                return v1.name.equals("Sales")
                    || v2 == 1;
              }
            }).count());

    assertEquals(
        2, Linq4j.asEnumerable(depts).asQueryable().skip(1).count());
    assertEquals(
        0, Linq4j.asEnumerable(depts).asQueryable().skip(5).count());
    assertEquals(
        1,
        Linq4j.asEnumerable(depts).asQueryable().skipWhileN(
            Expressions.<Predicate2<Department, Integer>>lambda(
                new Predicate2<Department, Integer>() {
                  public boolean apply(Department v1, Integer v2) {
                    return v1.name.equals("Sales")
                           || v2 == 1;
                  }
                })).count());
  }

  @Test public void testOrderBy() {
    // Note: sort is stable. Records occur Fred, Eric, Janet in input.
    assertEquals(
        "[Employee(name: Fred, deptno:10),"
        + " Employee(name: Eric, deptno:10),"
        + " Employee(name: Janet, deptno:10),"
        + " Employee(name: Bill, deptno:30)]",
        Linq4j.asEnumerable(emps).orderBy(EMP_DEPTNO_SELECTOR)
            .toList().toString());
  }

  @Test public void testOrderByComparator() {
    assertEquals(
        "[Employee(name: Bill, deptno:30),"
        + " Employee(name: Eric, deptno:10),"
        + " Employee(name: Fred, deptno:10),"
        + " Employee(name: Janet, deptno:10)]",
        Linq4j.asEnumerable(emps)
            .orderBy(EMP_NAME_SELECTOR)
            .orderBy(
                EMP_DEPTNO_SELECTOR, Collections.<Integer>reverseOrder())
            .toList().toString());
  }

  @Test public void testOrderByInSeries() {
    // OrderBy in series works because sort is stable.
    assertEquals(
        "[Employee(name: Eric, deptno:10),"
        + " Employee(name: Fred, deptno:10),"
        + " Employee(name: Janet, deptno:10),"
        + " Employee(name: Bill, deptno:30)]",
        Linq4j.asEnumerable(emps)
            .orderBy(EMP_NAME_SELECTOR)
            .orderBy(EMP_DEPTNO_SELECTOR)
            .toList().toString());
  }

  @Test public void testOrderByDescending() {
    assertEquals(
        "[Employee(name: Janet, deptno:10),"
        + " Employee(name: Fred, deptno:10),"
        + " Employee(name: Eric, deptno:10),"
        + " Employee(name: Bill, deptno:30)]",
        Linq4j.asEnumerable(emps)
            .orderByDescending(EMP_NAME_SELECTOR)
            .toList().toString());
  }

  @Test public void testReverse() {
    assertEquals(
        "[Employee(name: Janet, deptno:10),"
        + " Employee(name: Eric, deptno:10),"
        + " Employee(name: Bill, deptno:30),"
        + " Employee(name: Fred, deptno:10)]",
        Linq4j.asEnumerable(emps)
            .reverse()
            .toList()
            .toString());
  }

  @Test public void testList0() {
    final List<Employee> employees = Arrays.asList(
        new Employee(100, "Fred", 10),
        new Employee(110, "Bill", 30),
        new Employee(120, "Eric", 10),
        new Employee(130, "Janet", 10));
    final List<Employee> result = new ArrayList<Employee>();
    Linq4j.asEnumerable(employees)
        .where(
            new Predicate1<Employee>() {
              public boolean apply(Employee e) {
                return e.name.contains("e");
              }
            })
        .into(result);
    assertEquals(
        "[Employee(name: Fred, deptno:10), Employee(name: Janet, deptno:10)]",
        result.toString());
  }

  @Test public void testList() {
    final List<Employee> employees = Arrays.asList(
        new Employee(100, "Fred", 10),
        new Employee(110, "Bill", 30),
        new Employee(120, "Eric", 10),
        new Employee(130, "Janet", 10));
    final Map<Employee, Department> empDepts =
        new HashMap<Employee, Department>();
    for (Employee employee : employees) {
      empDepts.put(employee, depts[(employee.deptno - 10) / 10]);
    }
    final List<Grouping<Object, Map.Entry<Employee, Department>>> result =
        new ArrayList<Grouping<Object, Map.Entry<Employee, Department>>>();
    Linq4j.asEnumerable(empDepts.entrySet())
        .groupBy(
            new Function1<Map.Entry<Employee, Department>, Object>() {
              public Object apply(Map.Entry<Employee, Department> entry) {
                return entry.getValue();
              }
            })
        .into(result);
    assertNotNull(result.toString());
  }

  @Test public void testExample() {
    Linq4jExample.main(new String[0]);
  }

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

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + deptno;
      result = prime * result + empno;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Employee other = (Employee) obj;
      if (deptno != other.deptno) {
        return false;
      }
      if (empno != other.empno) {
        return false;
      }
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      return true;
    }
  }

  public static class Department {
    public final String name;
    public final int deptno;
    public final List<Employee> employees;

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

  // Cedric works in a non-existent department.
  //CHECKSTYLE: IGNORE 1
  public static final Employee[] badEmps = {
    new Employee(140, "Cedric", 40),
  };

  //CHECKSTYLE: IGNORE 1
  public static final Employee[] emps = {
    new Employee(100, "Fred", 10),
    new Employee(110, "Bill", 30),
    new Employee(120, "Eric", 10),
    new Employee(130, "Janet", 10),
  };

  //CHECKSTYLE: IGNORE 1
  public static final Department[] depts = {
    new Department("Sales", 10, Arrays.asList(emps[0], emps[2], emps[3])),
    new Department("HR", 20, Collections.<Employee>emptyList()),
    new Department("Marketing", 30, Arrays.asList(emps[1])),
  };
}

// End Linq4jTest.java
