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
package org.apache.calcite.sql;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Rewrites the parser-level {@code SELECT ... BY} clause into equivalent
 * {@code GROUP BY} and {@code ORDER BY} lists, and updates {@link SqlSelect}
 * accordingly.
 */
public final class SqlByRewriter {
  private SqlByRewriter() {}

  /**
   * Rewrites {@code by} when it is specified on {@code select}.  No action is
   * taken when {@code by} is null or empty.
   */
  public static void rewrite(SqlSelect select, @Nullable SqlNodeList by) {
    if (by == null || by.isEmpty()) {
      return;
    }
    ensureNoGroupBy(select);
    ensureNoOrderBy(select);

    select.setHasByClause(true);

    final SqlNodeList selectList = select.getSelectList();
    final SqlNodeList groupBy = new SqlNodeList(by.getParserPosition());
    final SqlNodeList orderBy = new SqlNodeList(by.getParserPosition());
    final List<SqlNode> extraSelectItems = new ArrayList<>();

    for (SqlNode node : by) {
      final SqlNode selectItem = SqlUtil.stripOrderModifiers(node);
      extraSelectItems.add(selectItem);

      final SqlNode groupItem = SqlUtil.stripAs(selectItem);
      groupBy.add(groupItem.clone(groupItem.getParserPosition()));

      final SqlNode orderItem = SqlUtil.stripAsFromOrder(node);
      orderBy.add(orderItem.clone(orderItem.getParserPosition()));
    }

    selectList.addAll(0, extraSelectItems);
    select.setGroupBy(groupBy);
    select.setOrderBy(orderBy);
  }

  private static void ensureNoGroupBy(SqlSelect select) {
    final SqlNodeList groupList = select.getGroup();
    if (groupList != null && !groupList.isEmpty()) {
      throw SqlUtil.newContextException(groupList.getParserPosition(),
          RESOURCE.selectByCannotWithGroupBy());
    }
  }

  private static void ensureNoOrderBy(SqlSelect select) {
    final SqlNodeList orderList = select.getOrderList();
    if (orderList != null && !orderList.isEmpty()) {
      throw SqlUtil.newContextException(orderList.getParserPosition(),
          RESOURCE.selectByCannotWithOrderBy());
    }
  }
}
