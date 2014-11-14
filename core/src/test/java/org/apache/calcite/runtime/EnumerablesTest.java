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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link net.hydromatic.optiq.runtime.Enumerables}.
 */
public class EnumerablesTest {
  @Test public void testSemiJoin() {
    assertThat(
        Enumerables.semiJoin(
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Emp(10, "Fred"),
                    new Emp(20, "Theodore"),
                    new Emp(20, "Sebastian"),
                    new Emp(30, "Joe"))),
            Linq4j.asEnumerable(
                Arrays.asList(
                    new Dept(20, "Sales"),
                    new Dept(15, "Marketing"))),
            new Function1<Emp, Integer>() {
              public Integer apply(Emp a0) {
                return a0.deptno;
              }
            },
            new Function1<Dept, Integer>() {
              public Integer apply(Dept a0) {
                return a0.deptno;
              }
            },
            Functions.<Integer>identityComparer()).toList().toString(),
        equalTo("[Emp(20, Theodore), Emp(20, Sebastian)]"));
  }

  /** Employee record. */
  private static class Emp {
    final int deptno;
    final String name;

    Emp(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Emp(" + deptno + ", " + name + ")";
    }
  }

  /** Department record. */
  private static class Dept {
    final int deptno;
    final String name;

    Dept(int deptno, String name) {
      this.deptno = deptno;
      this.name = name;
    }

    @Override public String toString() {
      return "Dept(" + deptno + ", " + name + ")";
    }
  }
}

// End EnumerablesTest.java
