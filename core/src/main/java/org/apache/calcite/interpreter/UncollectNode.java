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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.core.Uncollect;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Uncollect}.
 */
public class UncollectNode extends AbstractSingleNode<Uncollect> {

  public UncollectNode(Compiler compiler, Uncollect uncollect) {
    super(compiler, uncollect);
  }

  @Override public void run() throws InterruptedException {
    Row row = null;
    while ((row = source.receive()) != null) {
      for (Object value: row.getValues()) {
        if (value == null) {
          throw new NullPointerException("NULL value for unnest.");
        }
        int i = 1;
        if (value instanceof List) {
          List list = (List) value;
          for (Object o : list) {
            if (rel.withOrdinality) {
              sink.send(Row.of(o, i++));
            } else {
              sink.send(Row.of(o));
            }
          }
        } else if (value instanceof Map) {
          Map map = (Map) value;
          for (Object key : map.keySet()) {
            if (rel.withOrdinality) {
              sink.send(Row.of(key, map.get(key), i++));
            } else {
              sink.send(Row.of(key, map.get(key)));
            }
          }
        } else {
          throw new UnsupportedOperationException(
              String.format(Locale.ROOT,
                  "Invalid type: %s for unnest.",
                  value.getClass().getCanonicalName()));
        }
      }
    }
  }
}
