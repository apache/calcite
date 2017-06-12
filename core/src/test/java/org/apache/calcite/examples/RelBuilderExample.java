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
package org.apache.calcite.examples;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * Example that uses {@link org.apache.calcite.tools.RelBuilder}
 * to create various relational expressions.
 */
public class RelBuilderExample {
  private final boolean verbose;

  public RelBuilderExample(boolean verbose) {
    this.verbose = verbose;
  }

  public static void main(String[] args) {
    new RelBuilderExample(true).runAllExamples();
  }

  public void runAllExamples() {
    // Create a builder. The config contains a schema mapped
    // to the SCOTT database, with tables EMP and DEPT.
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    for (int i = 0; i < 4; i++) {
      doExample(builder, i);
      final RelNode node = builder.build();
      if (verbose) {
        System.out.println(RelOptUtil.toString(node));
      }
    }
  }

  private RelBuilder doExample(RelBuilder builder, int i) {
    switch (i) {
    case 0:
      return example0(builder);
    case 1:
      return example1(builder);
    case 2:
      return example2(builder);
    case 3:
      return example3(builder);
    case 4:
      return example4(builder);
    default:
      throw new AssertionError("unknown example " + i);
    }
  }

  /**
   * Creates a relational expression for a table scan.
   * It is equivalent to
   *
   * <blockquote><pre>SELECT *
   * FROM emp</pre></blockquote>
   */
  private RelBuilder example0(RelBuilder builder) {
    return builder
        .values(new String[] {"a", "b"}, 1, true, null, false);
  }

  /**
   * Creates a relational expression for a table scan.
   * It is equivalent to
   *
   * <blockquote><pre>SELECT *
   * FROM emp</pre></blockquote>
   */
  private RelBuilder example1(RelBuilder builder) {
    return builder
        .scan("EMP");
  }

  /**
   * Creates a relational expression for a table scan and project.
   * It is equivalent to
   *
   * <blockquote><pre>SELECT deptno, ename
   * FROM emp</pre></blockquote>
   */
  private RelBuilder example2(RelBuilder builder) {
    return builder
        .scan("EMP")
        .project(builder.field("DEPTNO"), builder.field("ENAME"));
  }

  /**
   * Creates a relational expression for a table scan, aggregate, filter.
   * It is equivalent to
   *
   * <blockquote><pre>SELECT deptno, count(*) AS c, sum(sal) AS s
   * FROM emp
   * GROUP BY deptno
   * HAVING count(*) &gt; 10</pre></blockquote>
   */
  private RelBuilder example3(RelBuilder builder) {
    return builder
        .scan("EMP")
        .aggregate(builder.groupKey("DEPTNO"),
            builder.count(false, "C"),
            builder.sum(false, "S", builder.field("SAL")))
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"),
                builder.literal(10)));
  }

  /**
   * Sometimes the stack becomes so deeply nested it gets confusing. To keep
   * things straight, you can remove expressions from the stack. For example,
   * here we are building a bushy join:
   *
   * <blockquote><pre>
   *                join
   *              /      \
   *         join          join
   *       /      \      /      \
   * CUSTOMERS ORDERS LINE_ITEMS PRODUCTS
   * </pre></blockquote>
   *
   * <p>We build it in three stages. Store the intermediate results in variables
   * `left` and `right`, and use `push()` to put them back on the stack when it
   * is time to create the final `Join`.
   */
  private RelBuilder example4(RelBuilder builder) {
    final RelNode left = builder
        .scan("CUSTOMERS")
        .scan("ORDERS")
        .join(JoinRelType.INNER, "ORDER_ID")
        .build();

    final RelNode right = builder
        .scan("LINE_ITEMS")
        .scan("PRODUCTS")
        .join(JoinRelType.INNER, "PRODUCT_ID")
        .build();

    return builder
        .push(left)
        .push(right)
        .join(JoinRelType.INNER, "ORDER_ID");
  }
}

// End RelBuilderExample.java
