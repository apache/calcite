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
package org.apache.calcite.benchmarks;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Compares {@link java.sql.Statement} vs {@link java.sql.PreparedStatement}.
 *
 * <p>This package contains micro-benchmarks to test calcite performance.
 *
 * <p>To run this and other benchmarks:
 *
 * <blockquote>
 *   <code>mvn package &amp;&amp;
 *   java -jar ./target/ubenchmarks.jar -wi 5 -i 5 -f 1</code>
 * </blockquote>
 *
 * <p>To run with profiling:
 *
 * <blockquote>
 *   <code>java -Djmh.stack.lines=10 -jar ./target/ubenchmarks.jar
 *     -prof hs_comp,hs_gc,stack -f 1 -wi 5</code>
 * </blockquote>
 */
public class StatementTest {

  /**
   * Connection to be used during tests.
   */
  @State(Scope.Thread)
  @BenchmarkMode(Mode.AverageTime)
  public static class HrConnection {
    Connection con;
    int id;
    HrSchema hr = new HrSchema();
    Random rnd = new Random();
    {
      try {
        Class.forName("org.apache.calcite.jdbc.Driver");
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(e);
      }
      Connection connection;

      try {
        Properties info = new Properties();
        info.put("lex", "JAVA");
        info.put("quoting", "DOUBLE_QUOTE");
        connection = DriverManager.getConnection("jdbc:calcite:", info);
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
      CalciteConnection calciteConnection;
      try {
        calciteConnection = connection.unwrap(CalciteConnection.class);
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
      final SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
      try {
        calciteConnection.setSchema("hr");
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
      con = connection;
    }

    @Setup(Level.Iteration)
    public void pickEmployee() {
      id = hr.emps[rnd.nextInt(4)].empid;
    }
  }

  /**
   * Tests performance of reused execution of prepared statement.
   */
  public static class HrPreparedStatement extends HrConnection {
    PreparedStatement ps;
    {
      try {
        ps = con.prepareStatement("select name from emps where empid = ?");
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Benchmark
  public String prepareBindExecute(HrConnection state) throws SQLException {
    Connection con = state.con;
    Statement st = null;
    ResultSet rs = null;
    String ename = null;
    try {
      final PreparedStatement ps =
          con.prepareStatement("select name from emps where empid = ?");
      st = ps;
      ps.setInt(1, state.id);
      rs = ps.executeQuery();
      rs.next();
      ename = rs.getString(1);
    } finally {
      close(rs, st);
    }
    return ename;
  }

  @Benchmark
  public String bindExecute(HrPreparedStatement state)
      throws SQLException {
    PreparedStatement st = state.ps;
    ResultSet rs = null;
    String ename = null;
    try {
      st.setInt(1, state.id);
      rs = st.executeQuery();
      rs.next();
      ename = rs.getString(1);
    } finally {
      close(rs, null); // Statement is not closed
    }
    return ename;
  }

  @Benchmark
  public String executeQuery(HrConnection state) throws SQLException {
    Connection con = state.con;
    Statement st = null;
    ResultSet rs = null;
    String ename = null;
    try {
      st = con.createStatement();
      rs = st.executeQuery("select name from emps where empid = " + state.id);
      rs.next();
      ename = rs.getString(1);
    } finally {
      close(rs, st);
    }
    return ename;
  }

  @Benchmark
  public String forEach(HrConnection state) throws SQLException {
    final Employee[] emps = state.hr.emps;
    for (Employee emp : emps) {
      if (emp.empid == state.id) {
        return emp.name;
      }
    }
    return null;
  }

  private static void close(ResultSet rs, Statement st) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (st != null) {
      try {
        st.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /** Pojo schema containing "emps" and "depts" tables. */
  public static class HrSchema {
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
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2])),
        new Department(30, "Marketing", Collections.<Employee>emptyList()),
        new Department(40, "HR", Collections.singletonList(emps[1])),
    };
  }

  /** Employee record. */
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

  /** Department record. */
  public static class Department {
    public final int deptno;
    public final String name;
    public final List<Employee> employees;

    public Department(
        int deptno, String name, List<Employee> employees) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
    }


    public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + "]";
    }
  }

}

// End StatementTest.java
