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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import org.junit.Test;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SqlValidatorFeatureTest verifies that features can be independently enabled
 * or disabled.
 */
public class SqlValidatorFeatureTest extends SqlValidatorTestCase {
  //~ Static fields/initializers ---------------------------------------------

  private static final String FEATURE_DISABLED = "feature_disabled";

  //~ Instance fields --------------------------------------------------------

  private Feature disabledFeature;

  //~ Constructors -----------------------------------------------------------

  public SqlValidatorFeatureTest() {
    super();
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(SqlTestFactory.INSTANCE.withValidator(FeatureValidator::new));
  }

  @Test public void testDistinct() {
    checkFeature(
        "select ^distinct^ name from dept",
        RESOURCE.sQLFeature_E051_01());
  }

  @Test public void testOrderByDesc() {
    checkFeature(
        "select name from dept order by ^name desc^",
        RESOURCE.sQLConformance_OrderByDesc());
  }

  // NOTE jvs 6-Mar-2006:  carets don't come out properly placed
  // for INTERSECT/EXCEPT, so don't bother

  @Test public void testIntersect() {
    checkFeature(
        "^select name from dept intersect select name from dept^",
        RESOURCE.sQLFeature_F302());
  }

  @Test public void testExcept() {
    checkFeature(
        "^select name from dept except select name from dept^",
        RESOURCE.sQLFeature_E071_03());
  }

  @Test public void testMultiset() {
    checkFeature(
        "values ^multiset[1]^",
        RESOURCE.sQLFeature_S271());

    checkFeature(
        "values ^multiset(select * from dept)^",
        RESOURCE.sQLFeature_S271());
  }

  @Test public void testTablesample() {
    checkFeature(
        "select name from ^dept tablesample bernoulli(50)^",
        RESOURCE.sQLFeature_T613());

    checkFeature(
        "select name from ^dept tablesample substitute('sample_dept')^",
        RESOURCE.sQLFeatureExt_T613_Substitution());
  }

  private void checkFeature(String sql, Feature feature) {
    // Test once with feature enabled:  should pass
    check(sql);

    // Test once with feature disabled:  should fail
    try {
      disabledFeature = feature;
      checkFails(sql, FEATURE_DISABLED);
    } finally {
      disabledFeature = null;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Extension to {@link SqlValidatorImpl} that validates features. */
  public class FeatureValidator extends SqlValidatorImpl {
    protected FeatureValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

    protected void validateFeature(
        Feature feature,
        SqlParserPos context) {
      if (feature.equals(disabledFeature)) {
        CalciteException ex =
            new CalciteException(
                FEATURE_DISABLED,
                null);
        if (context == null) {
          throw ex;
        }
        throw new CalciteContextException(
            "location",
            ex,
            context.getLineNum(),
            context.getColumnNum(),
            context.getEndLineNum(),
            context.getEndColumnNum());
      }
    }
  }
}

// End SqlValidatorFeatureTest.java
