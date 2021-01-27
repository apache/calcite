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

/**
 * Table that can be scanned without creating an intermediate relational expression.
 *
 * "不用创建 关系表达式 即可被扫描的表"
 *
 * > 这种方式基本不会用，原因是查询数据库的时候没有任何条件限制，默认会先把全部数据拉到内存，然后再根据filter条件在内存中过滤。
 */
public interface ScannableTable extends Table {

  /**
   * Returns an enumerator(枚举符) over the rows(行) in this Table.
   * Each row is represented as an array of its column values.
   *
   * fixme
   *      返回表中每一行的枚举符；
   *      使用数组代表每一行的值。
   */
  Enumerable<Object[]> scan(DataContext root);
}
