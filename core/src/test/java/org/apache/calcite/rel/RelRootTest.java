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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link RelRoot}.
 */
public class RelRootTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6877">[CALCITE-6877]
   * Generate LogicalProject in RelRoot.project() when mapping is not name trivial</a>. */
  @Test void testRelRootProjectForceNonNameTrivial() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defaultSchema =
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR);
    final FrameworkConfig frameworkConfig = RelBuilderTest.config()
        .defaultSchema(defaultSchema)
        .build();
    final RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
    final RelNode inputRel = relBuilder.scan("emps")
        .project(relBuilder.fields(Collections.singletonList("empid"))).build();

    final List<RelDataTypeField> fields =
        Collections.singletonList(
            // rename empid to empno via RelRoot
            new RelDataTypeFieldImpl("empno",
                inputRel.getRowType().getFieldList().get(0).getIndex(),
                inputRel.getRowType().getFieldList().get(0).getType()));

    final RelRoot root = RelRoot.of(inputRel, new RelRecordType(fields), SqlKind.SELECT);

    // inner LogicalProject selects one field and RelRoot only has one field
    assertThat(root.isRefTrivial(), is(true));

    // inner LogicalProject has different field name than RelRoot
    assertThat(root.isNameTrivial(), is(false));

    final RelNode project = root.project();
    assertThat(project, equalTo(inputRel));

    // regular project() and force project() are different
    final RelNode forceProject = root.project(true);
    assertThat(forceProject, not(equalTo(project)));

    // new LogicalProject on top of inputRel
    assertThat(forceProject, instanceOf(LogicalProject.class));
    assertThat(forceProject.getInput(0), equalTo(inputRel));

    // new LogicalProject renames field
    if (forceProject instanceof LogicalProject) {
      assertThat(((LogicalProject) forceProject).getNamedProjects().get(0).getValue(),
          equalTo("empno"));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6877">[CALCITE-6877]
   * Generate LogicalProject in RelRoot.project() when mapping is not name trivial</a>. */
  @Test void testRelRootProjectForceNameTrivial() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defaultSchema =
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR);
    final FrameworkConfig frameworkConfig = RelBuilderTest.config()
        .defaultSchema(defaultSchema)
        .build();
    final RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
    final RelNode inputRel = relBuilder.scan("emps")
        .project(relBuilder.fields(Collections.singletonList("empid"))).build();

    final RelRoot root = RelRoot.of(inputRel, SqlKind.SELECT);

    // inner LogicalProject selects one field and RelRoot only has one field
    assertThat(root.isRefTrivial(), is(true));

    // inner LogicalProject has same field name as RelRoot
    assertThat(root.isNameTrivial(), is(true));

    final RelNode project = root.project();
    assertThat(project, equalTo(inputRel));

    // regular project() and force project() are the same
    final RelNode forceProject = root.project(true);
    assertThat(forceProject, equalTo(project));
  }

  static SqlKind[] ddlSqlKinds() {
    return SqlKind.DDL.toArray(new SqlKind[0]);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7447">[CALCITE-7447]
   * RelRoot.project() adds Project for DDL nodes</a>. */
  @ParameterizedTest
  @MethodSource("ddlSqlKinds")
  void testRelRootProjectDdl(SqlKind ddlKind) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus defaultSchema =
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.HR);
    final FrameworkConfig frameworkConfig = RelBuilderTest.config()
        .defaultSchema(defaultSchema)
        .build();
    final RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
    final RelNode scanRel = relBuilder.scan("emps")
        .project(relBuilder.fields(Collections.singletonList("empid"))).build();

    final RelNode inputRel = new DummyDdlRelNode(relBuilder.getCluster(), scanRel);

    final RelRoot root = RelRoot.of(inputRel, ddlKind);

    final RelNode project = root.project();
    assertThat(project, equalTo(inputRel));
    assertThat(project, instanceOf(DummyDdlRelNode.class));

    // regular project() and force project() are the same
    final RelNode forceProject = root.project(true);
    assertThat(forceProject, equalTo(project));
  }

  /**
   * Dummy DDL RelNode for testing.
   */
  static class DummyDdlRelNode extends SingleRel {
    protected DummyDdlRelNode(RelOptCluster cluster, RelNode input) {
      this(cluster, cluster.traitSet(), input);
    }

    protected DummyDdlRelNode(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, traits, input);
    }
  }
}
