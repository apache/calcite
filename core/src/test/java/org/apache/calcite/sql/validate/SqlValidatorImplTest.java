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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.test.Fixtures;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests for {@link SqlValidatorImpl}. */
class SqlValidatorImplTest {
  private static final SqlParserPos POS = SqlParserPos.ZERO;

  @Test void testOrderByDoesNotClobberSelectOffsetFetch() {
    final SqlSelect selectWithOffset = createSelect(exactNumeric("2"), null);
    checkOrderByDoesNotClobberOffsetFetch(selectWithOffset, selectWithOffset);

    final SqlSelect selectWithFetch = createSelect(null, exactNumeric("3"));
    checkOrderByDoesNotClobberOffsetFetch(selectWithFetch, selectWithFetch);
  }

  @Test void testOrderByDoesNotClobberWithBodyOffsetFetch() {
    final SqlSelect selectWithOffset = createSelect(exactNumeric("2"), null);
    checkOrderByDoesNotClobberOffsetFetch(createWith(selectWithOffset),
        selectWithOffset);

    final SqlSelect selectWithFetch = createSelect(null, exactNumeric("3"));
    checkOrderByDoesNotClobberOffsetFetch(createWith(selectWithFetch),
        selectWithFetch);
  }

  private static SqlSelect createSelect(@Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    return new SqlSelect(POS, null, SqlNodeList.of(exactNumeric("1")), null,
        null, null, null, null, null, null, offset, fetch, null);
  }

  private static SqlWith createWith(SqlSelect body) {
    final SqlWithItem withItem = new SqlWithItem(POS,
        new SqlIdentifier("w", POS), null, createSelect(null, null),
        SqlLiteral.createBoolean(false, POS));
    return new SqlWith(POS, SqlNodeList.of(withItem), body);
  }

  private static void checkOrderByDoesNotClobberOffsetFetch(SqlNode query,
      SqlSelect innerSelect) {
    final @Nullable SqlNode innerOffset = innerSelect.getOffset();
    final @Nullable SqlNode innerFetch = innerSelect.getFetch();
    final SqlNode outerOffset = exactNumeric("4");
    final SqlNode outerFetch = exactNumeric("5");
    final SqlOrderBy orderBy = new SqlOrderBy(POS, query,
        SqlNodeList.of(exactNumeric("1")), outerOffset, outerFetch);
    final SqlValidatorImpl validator =
        (SqlValidatorImpl) Fixtures.forValidator().factory.createValidator();

    final SqlNode rewritten =
        validator.performUnconditionalRewrites(orderBy, false);

    assertThat(rewritten, instanceOf(SqlSelect.class));
    final SqlSelect outerSelect = (SqlSelect) rewritten;
    assertSame(query, outerSelect.getFrom());
    assertSame(innerOffset, innerSelect.getOffset());
    assertSame(innerFetch, innerSelect.getFetch());
    assertSame(outerOffset, outerSelect.getOffset());
    assertSame(outerFetch, outerSelect.getFetch());
  }

  private static SqlNode exactNumeric(String value) {
    return SqlLiteral.createExactNumeric(value, POS);
  }
}
