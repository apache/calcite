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
package org.apache.calcite.runtime;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.util.CursorInput;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

/** Utilities for working with {@link CursorInput}. */
public final class CursorInputs {
  private CursorInputs() {
  }

  public static CursorInput of(RelDataType rowType, Enumerable<Object[]> rows) {
    return new CursorInput() {
      @Override public RelDataType getRowType() {
        return rowType;
      }

      @Override public Enumerable<Object[]> rows() {
        return rows;
      }
    };
  }

  public static ResultSet metadataOnly(List<ColumnMetaData> columns) {
    return resultSet(new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        throw new UnsupportedOperationException(
            "Cursor rows are unavailable during type inference");
      }
    }, columns, TimeZone.getTimeZone("UTC"));
  }

  /** Wraps rows and prepared column metadata in a lazy, read-only,
   * forward-only result set. Closing it closes the input enumerator. */
  public static ResultSet resultSet(Enumerable<Object[]> input,
      List<ColumnMetaData> columns, TimeZone timeZone) {
    final Meta.Signature signature =
        Meta.Signature.create(columns, "", Collections.emptyList(),
            Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT);
    try {
      final AvaticaResultSet resultSet =
          new AvaticaResultSet(null, null, signature,
              new AvaticaResultSetMetaData(null, null, signature), timeZone, null) {
            @Override public int getType() {
              return TYPE_FORWARD_ONLY;
            }

            @Override public int getConcurrency() {
              return CONCUR_READ_ONLY;
            }

            @Override public int getHoldability() {
              return CLOSE_CURSORS_AT_COMMIT;
            }

            @Override public int getFetchDirection() {
              return FETCH_FORWARD;
            }
          };
      return resultSet.execute2(
          new ArrayEnumeratorCursor(new Enumerator<Object[]>() {
            private @Nullable Enumerator<Object[]> rows;

            @Override public Object[] current() {
              return requireNonNull(rows, "rows").current();
            }

            @Override public boolean moveNext() {
              if (rows == null) {
                rows = input.enumerator();
              }
              return rows.moveNext();
            }

            @Override public void reset() {
              throw new UnsupportedOperationException();
            }

            @Override public void close() {
              if (rows != null) {
                rows.close();
                rows = null;
              }
            }
          }), columns);
    } catch (SQLException e) {
      throw Util.throwAsRuntime(e);
    }
  }

  /** Invokes a table function with fresh result set arguments for each enumeration.
   * Closes the arguments when the output is exhausted, closed, or fails. */
  public static <T> Enumerable<T> enumerable(CursorInput[] inputs,
      List<? extends List<ColumnMetaData>> columns, TimeZone timeZone,
      Function1<ResultSet[], Enumerable<T>> function) {
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        final ResultSet[] resultSets = new ResultSet[inputs.length];
        final Enumerator<T> output;
        try {
          for (int i = 0; i < inputs.length; i++) {
            resultSets[i] = resultSet(inputs[i].rows(), columns.get(i), timeZone);
          }
          output = function.apply(resultSets).enumerator();
        } catch (RuntimeException | Error e) {
          close(resultSets, e);
          throw e;
        }
        return new Enumerator<T>() {
          private boolean closed;

          @Override public T current() {
            try {
              return output.current();
            } catch (RuntimeException | Error e) {
              closeOnFailure(e);
              throw e;
            }
          }

          @Override public boolean moveNext() {
            if (closed) {
              return false;
            }
            try {
              if (output.moveNext()) {
                return true;
              }
              close();
              return false;
            } catch (RuntimeException | Error e) {
              closeOnFailure(e);
              throw e;
            }
          }

          @Override public void reset() {
            throw new UnsupportedOperationException("Cursor arguments cannot be reset");
          }

          @Override public void close() {
            if (!closed) {
              closed = true;
              Throwable failure = null;
              try {
                output.close();
              } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
              } finally {
                CursorInputs.close(resultSets, failure);
              }
            }
          }

          private void closeOnFailure(Throwable failure) {
            try {
              close();
            } catch (RuntimeException | Error e) {
              failure.addSuppressed(e);
            }
          }
        };
      }
    };
  }

  private static void close(ResultSet[] resultSets, @Nullable Throwable failure) {
    Throwable first = failure;
    for (ResultSet resultSet : resultSets) {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException | RuntimeException | Error e) {
          if (first == null) {
            first = e;
          } else {
            first.addSuppressed(e);
          }
        }
      }
    }
    if (failure == null && first != null) {
      throw Util.throwAsRuntime(first);
    }
  }

}
