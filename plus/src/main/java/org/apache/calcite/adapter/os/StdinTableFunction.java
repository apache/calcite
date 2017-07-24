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
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

/**
 * Table function that reads stdin and returns one row per line.
 */
public class StdinTableFunction {

  private StdinTableFunction() {}

  public static ScannableTable eval(boolean b) {
    return new ScannableTable() {
      public Enumerable<Object[]> scan(DataContext root) {
        final InputStream is = DataContext.Variable.STDIN.get(root);
        return new AbstractEnumerable<Object[]>() {
          final InputStreamReader in =
              new InputStreamReader(is, StandardCharsets.UTF_8);
          final BufferedReader br = new BufferedReader(in);
          public Enumerator<Object[]> enumerator() {
            return new Enumerator<Object[]>() {
              String line;
              int i;

              public Object[] current() {
                if (line == null) {
                  throw new NoSuchElementException();
                }
                return new Object[] {i, line};
              }

              public boolean moveNext() {
                try {
                  line = br.readLine();
                  ++i;
                  return line != null;
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              public void reset() {
                throw new UnsupportedOperationException();
              }

              public void close() {
                try {
                  br.close();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
        };
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("ordinal", SqlTypeName.INTEGER)
            .add("line", SqlTypeName.VARCHAR)
            .build();
      }

      public Statistic getStatistic() {
        return Statistics.of(1000d, ImmutableList.of(ImmutableBitSet.of(1)));
      }

      public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
      }

      public boolean isRolledUp(String column) {
        return false;
      }

      public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          SqlNode parent, CalciteConnectionConfig config) {
        return true;
      }
    };
  }
}

// End StdinTableFunction.java
