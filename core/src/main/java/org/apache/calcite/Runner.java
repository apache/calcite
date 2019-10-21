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
package org.apache.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.custom.AsscomInnerOuterRule;
import org.apache.calcite.rel.rules.custom.AsscomOuterInnerRule;
import org.apache.calcite.rel.rules.custom.AsscomOuterOuterRule;
import org.apache.calcite.rel.rules.custom.AssocInnerOuterRule;
import org.apache.calcite.rel.rules.custom.AssocOuterInnerRule;
import org.apache.calcite.rel.rules.custom.NullifyJoinRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

/**
 * A runner class for manual testing.
 */
public class Runner {
  // Defines the default dialect used in this class.
  private static final SqlDialect DEFAULT_DIALECT
      = SqlDialect.DatabaseProduct.MYSQL.getDialect();

  // A counter for the number of transactions executed so far.
  private static int count = 1;

  public static void main(String[] args) throws Exception {
    // 1. A single inner join.
    String sqlQuery = "select e.name, d.depName "
        + "from p.employees e join p.departments d on e.depID = d.depID";
    Program programs = Programs.ofRules(
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, false);

    // 2. A single left outer join.
    sqlQuery = "select e.name, d.depName "
        + "from p.employees e left join p.departments d on e.depID = d.depID";
    programs = Programs.ofRules(
        NullifyJoinRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, false);

    // 3. Two joins (left outer join + inner join) - for Rule 21.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "inner join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocOuterInnerRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, true);

    // 4. Two joins (inner join + left outer join) - for Rule 22.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.departments d "
        + "inner join p.employees e on d.depID = e.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocInnerOuterRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, true);

    // 5. Two joins (left outer join + inner join) - for Rule 23.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "inner join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomOuterInnerRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, true);

    // 6. Two joins (inner join + left outer join) - for Rule 24.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "inner join p.departments d on e.depID = d.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomInnerOuterRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, true);

    // 7. Two joins (left outer join + left outer join) - for Rule 25.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomOuterOuterRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    buildAndTransformQuery(programs, sqlQuery, true);
  }

  /**
   * This method emulates the whole life cycle of a given SQL query: parse, validate build and
   * transform. It will close the planner after usage.
   *
   * @param programs is the set of transformation rules to be used.
   * @param sqlQuery is the original SQL query in its string representation.
   * @param ignoreTypeCheck indicates whether type check should be turned off.
   * @throws Exception when there is error during any step.
   */
  private static void buildAndTransformQuery(final Program programs,
      final String sqlQuery, final boolean ignoreTypeCheck) throws Exception {
    // Builds the schema.
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defaultSchema = rootSchema.add("p", new ReflectiveSchema(new People()));

    // Creates the planner.
    final SqlParser.Config parserConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(defaultSchema)
        .programs(programs)
        .build();
    final Planner planner = Frameworks.getPlanner(config);

    System.out.println("============================ Start ============================");
    System.out.println("Transaction ID: " + count++ + "\n");

    // Prints the original SQL query string.
    System.out.println("Input query:");
    System.out.println(sqlQuery + "\n");
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = true;
    }

    // Parses, validates and builds the query.
    final SqlNode parse = planner.parse(sqlQuery);
    final SqlNode validate = planner.validate(parse);
    final RelNode relNode = planner.rel(validate).rel;
    System.out.println("Before transformation:");
    System.out.println(RelOptUtil.toString(relNode));

    // Transforms the query.
    RelTraitSet traitSet = relNode.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transformedNode = planner.transform(0, traitSet, relNode);
    System.out.println("After transformation:");
    System.out.println(RelOptUtil.toString(transformedNode));
    if (ignoreTypeCheck) {
      RelOptUtil.disableTypeCheck = false;
    }

    // Converts the transformed relational expression back to SQL query string.
    final RelToSqlConverter converter = new RelToSqlConverter(DEFAULT_DIALECT);
    final SqlNode transformedSqlNode = converter.visitChild(0, transformedNode).asStatement();
    final String transformedSqlQuery = transformedSqlNode.toSqlString(DEFAULT_DIALECT).getSql();
    System.out.println("Output query:");
    System.out.println(transformedSqlQuery + "\n");
    System.out.println("============================= End =============================\n");

    // Closes the planner.
    planner.close();
  }

  /**
   *  Represents the database named company. */
  public static class People {
    public final Employee[] employees = {
        new Employee(10, 1, "Daniel"),
        new Employee(20, 1, "Mark"),
        new Employee(30, 2, "Smith"),
        new Employee(40, 3, "Armstrong"),
        new Employee(50, 2, "Gabriel"),
        new Employee(60, 5, "Daniel"),
        new Employee(70, 7, "Joe"),
        new Employee(80, 2, "Kim"),
        new Employee(90, 1, "Gino")
    };

    public final Department[] departments = {
        new Department(1, "Engineering", 100),
        new Department(2, "Finance", 100)
    };

    public final Company[] companies = {
        new Company(100, "All Link Pte Ltd"),
        new Company(200, "")
    };
  }

  /**
   *  Represents the schema of the employee table. */
  public static class Employee {
    public final int empID;
    public final int depID;
    public final String name;

    Employee(int empID, int depID, String name) {
      this.empID = empID;
      this.depID = depID;
      this.name = name;
    }
  }

  /**
   *  Represents the schema of the department table. */
  public static class Department {
    public final int depID;
    public final String depName;
    public final int cmpID;

    Department(int depID, String depName, int cmpID) {
      this.depID = depID;
      this.depName = depName;
      this.cmpID = cmpID;
    }
  }

  /**
   *  Represents the schema of the company table. */
  public static class Company {
    public final int cmpID;
    public final String cmpName;

    Company(int cmpID, String cmpName) {
      this.cmpID = cmpID;
      this.cmpName = cmpName;
    }
  }

  private Runner() {
  }
}

// End Runner.java
