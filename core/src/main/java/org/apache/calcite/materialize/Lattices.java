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
package org.apache.calcite.materialize;

/**
 * Utilities for {@link Lattice}, {@link LatticeStatisticProvider}.
 *
 * fixme Lattice 和 LatticeStatisticProvider(数据统计) 的工具类。
 */
public class Lattices {

  // 防止被实例化，因为此类提供的都是全局静态实例。
  private Lattices() {}

  /**
   * Statistics provider that uses SQL.
   *
   * 使用sql的 统计类提供者。
   */
  public static final LatticeStatisticProvider.Factory SQL = SqlLatticeStatisticProvider.FACTORY;

  /**
   * Statistics provider that uses SQL then stores the results in a cache.
   *
   * 使用说起来的统计提供者，将数据放到cache中。
   */
  public static final LatticeStatisticProvider.Factory CACHED_SQL = SqlLatticeStatisticProvider.CACHED_FACTORY;

  /**
   * Statistics provider that uses a profiler.
   *
   * 使用 ProfilerLatticeStatisticProvider 的数据提供者。
   *
   * profiler n: 分析器。
   */
  public static final LatticeStatisticProvider.Factory PROFILER =  ProfilerLatticeStatisticProvider.FACTORY;
}
