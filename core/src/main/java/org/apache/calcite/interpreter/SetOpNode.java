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

import org.apache.calcite.rel.core.SetOp;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.HashSet;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.SetOp},
 * including {@link org.apache.calcite.rel.core.Minus},
 * {@link org.apache.calcite.rel.core.Union} and
 * {@link org.apache.calcite.rel.core.Intersect}.
 */
public class SetOpNode implements Node {
  private final ImmutableList<Source> sources;
  private final Sink sink;
  private final SetOp setOp;

  public SetOpNode(Compiler compiler, SetOp setOp) {
    ImmutableList.Builder<Source> builder = ImmutableList.builder();
    for (int i = 0; i < setOp.getInputs().size(); i++) {
      builder.add(compiler.source(setOp, i));
    }
    sources = builder.build();
    sink = compiler.sink(setOp);
    this.setOp = setOp;
  }

  @Override public void close() {
    for (Source source : sources) {
      source.close();
    }
  }

  @Override public void run() throws InterruptedException {
    final Collection<Row> rows = readRows(sources.get(0));
    switch (setOp.kind) {
    case INTERSECT:
      for (Source source : sources.subList(1, sources.size())) {
        intersect(rows, readRows(source));
      }
      break;
    case EXCEPT:
      for (Source source : sources.subList(1, sources.size())) {
        except(rows, readRows(source));
      }
      break;
    case UNION:
      for (Source source : sources.subList(1, sources.size())) {
        rows.addAll(readRows(source));
      }
      break;
    default:
      return;
    }
    for (Row row : rows) {
      sink.send(row);
    }
  }

  private void intersect(Collection<Row> rows, Collection<Row> rows2) {
    final Collection<Row> result = newCollection();
    for (Row row : rows) {
      if (rows2.remove(row)) {
        result.add(row);
      }
    }
    rows.clear();
    rows.addAll(result);
  }

  private void except(Collection<Row> rows, Collection<Row> rows2) {
    final Collection<Row> result = newCollection();
    for (Row row : rows) {
      if (!rows2.remove(row)) {
        result.add(row);
      }
    }
    rows.clear();
    rows.addAll(result);
  }

  private Collection<Row> readRows(Source source) {
    final Collection<Row> rows = newCollection();
    Row row;
    while ((row = source.receive()) != null) {
      rows.add(row);
    }
    return rows;
  }

  private Collection<Row> newCollection() {
    if (setOp.all) {
      return HashMultiset.create();
    }
    return new HashSet<>();
  }
}
