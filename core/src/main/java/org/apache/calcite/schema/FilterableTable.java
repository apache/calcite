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
package org.apache.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Table that can be scanned,
 * optionally applying supplied filter expressions,
 * without creating an intermediate relational expression.
 * fixme
 *      可以被扫描的表；
 *      可以用提供的过滤表达式，而不创建中间关系表达式。
 *
 * @see ScannableTable
 */
public interface FilterableTable extends Table {

  /**
   * Returns an enumerator over the rows in this Table.
   * Each row is represented as an array of its column values.
   * fixme
   *      返回代表 表中行 的枚举，每一行使用一个数组表示、数组值是对应的列值。
   *
   * <p>
   *   The list of filters is mutable.
   *   If the table can implement a particular filter, it should remove that
   *   filter from the list.
   *   If it cannot implement a filter, it should leave it in the list.
   *   Any filters remaining will be implemented by the consuming Calcite
   *   operator.
   *   fixme
   *        过滤器的列表是可变的。
   */
  Enumerable<Object[]> scan(DataContext root, List<RexNode> filters);
}
