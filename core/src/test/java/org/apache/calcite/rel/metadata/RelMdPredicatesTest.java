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

package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test of {@link RelMdPredicates}
 */
public class RelMdPredicatesTest {

  /**
   * Test operator. This operator is undeterministic.
   */
  class UndeterministicOperator extends SqlSpecialOperator {

    public UndeterministicOperator() {
      super("UDC", SqlKind.OTHER_FUNCTION, 0, false,
          ReturnTypes.BOOLEAN, null, null);
    }

    @Override public boolean isDeterministic() {
      return false;
    }
  }

  private final SqlOperator udc = new UndeterministicOperator();

  @Test public void testGetPredicatesForJoin() throws Exception {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode join = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER, builder.call(udc))
        .build();
    RelMetadataQuery mq = RelMetadataQuery.instance();
    Assert.assertTrue(mq.getPulledUpPredicates(join).pulledUpPredicates.isEmpty());

    RelNode join1 = builder
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
          builder.call(SqlStdOperatorTable.EQUALS,
            builder.field(2, 0, 0),
            builder.field(2, 1, 0)))
        .build();
    Assert.assertEquals("=($0, $8)",
        mq.getPulledUpPredicates(join1).pulledUpPredicates.get(0).toString());
  }

  @Test public void testGetPredicatesForFilter() throws Exception {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    RelNode filter = builder
        .scan("EMP")
        .filter(builder.call(udc))
        .build();
    RelMetadataQuery mq = RelMetadataQuery.instance();
    Assert.assertTrue(mq.getPulledUpPredicates(filter).pulledUpPredicates.isEmpty());

    RelNode filter1 = builder
        .scan("EMP")
        .filter(
          builder.call(SqlStdOperatorTable.EQUALS,
            builder.field(1, 0, 0),
            builder.field(1, 0, 1)))
        .build();
    Assert.assertEquals("=($0, $1)",
        mq.getPulledUpPredicates(filter1).pulledUpPredicates.get(0).toString());
  }
}
// End RelMdPredicatesTest.java
