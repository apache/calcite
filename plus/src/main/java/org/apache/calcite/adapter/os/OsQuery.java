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
package org.apache.calcite.adapter.os;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Enumerator that reads from OS's System.
 */
public class OsQuery implements Enumerator<Object[]> {
  private static final Logger LOGGER = CalciteTrace.getParserTracer();

  private final Enumerator<Object[]> enumerator;

  public OsQuery(String type) {
    this.enumerator = Linq4j.enumerator(eval(type));
  }

  public Enumerator<Object[]> getEnumerator() {
    return enumerator;
  }

  @Override public Object[] current() {
    return enumerator.current();
  }

  @Override public boolean moveNext() {
    return enumerator.moveNext();
  }

  @Override public void reset() {
    enumerator.reset();
  }

  @Override public void close() {
    enumerator.close();
  }

  public List<Object[]> eval(String type) {
    OsQueryType queryType = OsQueryType.valueOf(type.toUpperCase(Locale.ROOT));
    try {
      return queryType.getInfo();
    } catch (Exception e) {
      LOGGER.error("Failed to get result's info", e);
      return new ArrayList<>();
    }
  }
}
