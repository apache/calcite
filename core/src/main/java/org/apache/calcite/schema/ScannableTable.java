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
 * "不用创建关系表达式即可被扫描的表"
 */
public interface ScannableTable extends Table {

  /**
   * Returns an enumerator(枚举符) over the rows(行) in this Table.
   * Each row is represented as an array of its column values.
   *
   * ？？返回该表每行的枚举符，每一行代表该列的值数组？？
   */
  Enumerable<Object[]> scan(DataContext root);
}
