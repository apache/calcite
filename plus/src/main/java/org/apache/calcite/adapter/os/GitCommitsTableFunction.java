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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.NoSuchElementException;

import static java.lang.Long.parseLong;

/**
 * Table function that executes the OS "git log" command
 * to discover git commits.
 */
public class GitCommitsTableFunction {

  /** An example of the timestamp + offset at the end of author and committer
   * fields. */
  private static final String TS_OFF = "1500769547 -0700";

  /** An example of the offset at the end of author and committer fields. */
  private static final String OFF = "-0700";

  private GitCommitsTableFunction() {}

  public static ScannableTable eval(boolean b) {
    return new AbstractBaseScannableTable() {
      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        final Enumerable<String> enumerable =
            Processes.processLines("git", "log", "--pretty=raw");
        return new AbstractEnumerable<@Nullable Object[]>() {
          @Override public Enumerator<@Nullable Object[]> enumerator() {
            final Enumerator<String> e = enumerable.enumerator();
            return new Enumerator<@Nullable Object[]>() {
              private @Nullable Object @Nullable [] objects;
              private final StringBuilder b = new StringBuilder();

              @Override public @Nullable Object[] current() {
                if (objects == null) {
                  throw new NoSuchElementException();
                }
                return objects;
              }

              @Override public boolean moveNext() {
                if (!e.moveNext()) {
                  objects = null;
                  return false;
                }
                objects = new Object[9];
                for (;;) {
                  final String line = e.current();
                  if (line.isEmpty()) {
                    break; // next line will be start of comments
                  }
                  if (line.startsWith("commit ")) {
                    objects[0] = line.substring("commit ".length());
                  } else if (line.startsWith("tree ")) {
                    objects[1] = line.substring("tree ".length());
                  } else if (line.startsWith("parent ")) {
                    if (objects[2] == null) {
                      objects[2] = line.substring("parent ".length());
                    } else {
                      objects[3] = line.substring("parent ".length());
                    }
                  } else if (line.startsWith("author ")) {
                    objects[4] =
                        line.substring("author ".length(),
                            line.length() - TS_OFF.length() - 1);
                    objects[5] =
                        parseLong(
                            line.substring(line.length() - TS_OFF.length(),
                            line.length() - OFF.length() - 1)) * 1000;
                  } else if (line.startsWith("committer ")) {
                    objects[6] =
                        line.substring("committer ".length(),
                            line.length() - TS_OFF.length() - 1);
                    objects[7] =
                        parseLong(
                            line.substring(line.length() - TS_OFF.length(),
                            line.length() - OFF.length() - 1)) * 1000;
                  }
                  if (!e.moveNext()) {
                    // We have a row, and it's the last because input is empty
                    return true;
                  }
                }
                for (;;) {
                  if (!e.moveNext()) {
                    // We have a row, and it's the last because input is empty
                    objects[8] = b.toString();
                    b.setLength(0);
                    return true;
                  }
                  final String line = e.current();
                  if (line.isEmpty()) {
                    // We're seeing the empty line at the end of message
                    objects[8] = b.toString();
                    b.setLength(0);
                    return true;
                  }
                  b.append(line.substring("    ".length())).append("\n");
                }
              }

              @Override public void reset() {
                throw new UnsupportedOperationException();
              }

              @Override public void close() {
                e.close();
              }
            };
          }
        };
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("commit", SqlTypeName.CHAR, 40)
            .add("tree", SqlTypeName.CHAR, 40)
            .add("parent", SqlTypeName.CHAR, 40)
            .add("parent2", SqlTypeName.CHAR, 40)
            .add("author", SqlTypeName.VARCHAR)
            .add("author_timestamp", SqlTypeName.TIMESTAMP)
            .add("committer", SqlTypeName.VARCHAR)
            .add("commit_timestamp", SqlTypeName.TIMESTAMP)
            .add("message", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }
}
