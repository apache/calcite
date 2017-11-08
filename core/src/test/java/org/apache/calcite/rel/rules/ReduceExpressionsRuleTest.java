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
package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexUtil.ExprSimplifier;
import org.apache.calcite.test.RexImplicationCheckerTest.Fixture;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Unit tests for {@link ReduceExpressionsRule} algorithms. */
public class ReduceExpressionsRuleTest {

  @Test public void testSimplifyCastMatchNullability() {
    final Fixture f = new Fixture();
    final ExprSimplifier defaultSimplifier = new RexUtil.ExprSimplifier(f.simplify);
    final ExprSimplifier nonMatchingNullabilitySimplifier =
        new RexUtil.ExprSimplifier(f.simplify, false);

    // The cast is nullable, while the literal is not nullable. When we simplify
    // it, we end up with the literal. If defaultSimplifier is used, a CAST is
    // introduced on top of the expression, as nullability of the new expression
    // does not match the nullability of the original one. If
    // nonMatchingNullabilitySimplifier is used, the CAST is not added and the
    // simplified expression only consists of the literal.
    final RexNode e = f.cast(f.intRelDataType, f.literal(2014));
    assertThat(defaultSimplifier.apply(e).toString(),
        is("CAST(2014):JavaType(class java.lang.Integer)"));
    assertThat(nonMatchingNullabilitySimplifier.apply(e).toString(),
        is("2014"));

    // In this case, the cast is not nullable. Thus, in both cases, the
    // simplified expression only consists of the literal.
    RelDataType notNullIntRelDataType = f.typeFactory.createJavaType(int.class);
    final RexNode e2 = f.cast(notNullIntRelDataType,
        f.cast(notNullIntRelDataType, f.literal(2014)));
    assertThat(defaultSimplifier.apply(e2).toString(),
        is("2014"));
    assertThat(nonMatchingNullabilitySimplifier.apply(e2).toString(),
        is("2014"));
  }

}

// End ReduceExpressionsRuleTest.java
