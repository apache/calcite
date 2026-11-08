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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.test.SqlTestFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link AbstractNamespace}. */
class AbstractNamespaceTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7792">[CALCITE-7792]
   * Failed namespace validation breaks SqlAdvisorValidator hints and can
   * overflow the stack in getRowType()</a>.
   *
   * <p>After {@code validateImpl()} fails, both {@code getRowType()} and
   * {@code getType()} re-throw the original exception. */
  @Test void testFailureIsRethrownByGetRowTypeAndGetType() {
    final FailingNamespace namespace = newNamespace();
    final RuntimeException failure =
        assertThrows(RuntimeException.class,
            () -> namespace.validate(unknownType(namespace)));
    assertThat(failure, sameInstance(namespace.failure));

    assertThat(assertThrows(RuntimeException.class, namespace::getRowType),
        sameInstance(namespace.failure));
    assertThat(assertThrows(RuntimeException.class, namespace::getType),
        sameInstance(namespace.failure));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7792">[CALCITE-7792]
   * Failed namespace validation breaks SqlAdvisorValidator hints and can
   * overflow the stack in getRowType()</a>.
   *
   * <p>A namespace whose validation failed is marked VALID, so it is not
   * validated a second time; {@code validateImpl()} may already have changed
   * the validator's state. */
  @Test void testFailedNamespaceIsNotValidatedAgain() {
    final FailingNamespace namespace = newNamespace();
    assertThrows(RuntimeException.class,
        () -> namespace.validate(unknownType(namespace)));
    assertThat(namespace.validateImplCount, is(1));

    // Validating again is a no-op; the error comes from the row type.
    namespace.validate(unknownType(namespace));
    assertThrows(RuntimeException.class, namespace::getRowType);
    assertThat(namespace.validateImplCount, is(1));
  }

  private static FailingNamespace newNamespace() {
    return new FailingNamespace(
        (SqlValidatorImpl) SqlTestFactory.INSTANCE.createValidator());
  }

  private static RelDataType unknownType(FailingNamespace namespace) {
    return ((SqlValidatorImpl) namespace.getValidator()).unknownType;
  }

  /** Namespace whose validation always fails. */
  private static class FailingNamespace extends AbstractNamespace {
    final RuntimeException failure = new IllegalStateException("cannot validate");
    int validateImplCount;

    FailingNamespace(SqlValidatorImpl validator) {
      super(validator, null);
    }

    @Override protected RelDataType validateImpl(RelDataType targetRowType) {
      ++validateImplCount;
      throw failure;
    }

    @Override public @Nullable SqlNode getNode() {
      return null;
    }
  }
}
