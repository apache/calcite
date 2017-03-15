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
package org.apache.calcite.avatica.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

/**
 * All we know about a statement. Encapsulates a {@link ResultSet}.
 */
public class StatementInfo {
  private volatile Boolean relativeSupported = null;

  final Statement statement; // sometimes a PreparedStatement
  private ResultSet resultSet;
  private long position = 0;

  // True when setResultSet(ResultSet) is called to let us determine the difference between
  // a null ResultSet (from an update) from the lack of a ResultSet.
  private boolean resultsInitialized = false;

  public StatementInfo(Statement statement) {
    // May be null when coming from a DatabaseMetaData call
    this.statement = statement;
  }

  // Visible for testing
  void setPosition(long position) {
    this.position = position;
  }

  // Visible for testing
  long getPosition() {
    return this.position;
  }

  /**
   * Set a ResultSet on this object.
   *
   * @param resultSet The current ResultSet
   */
  public void setResultSet(ResultSet resultSet) {
    resultsInitialized = true;
    this.resultSet = resultSet;
  }

  /**
   * @return The {@link ResultSet} for this Statement, may be null.
   */
  public ResultSet getResultSet() {
    return this.resultSet;
  }

  /**
   * @return True if {@link #setResultSet(ResultSet)} was ever invoked.
   */
  public boolean isResultSetInitialized() {
    return resultsInitialized;
  }

  /**
   * @see ResultSet#next()
   */
  public boolean next() throws SQLException {
    return _next(resultSet);
  }

  boolean _next(ResultSet results) throws SQLException {
    boolean ret = results.next();
    position++;
    return ret;
  }

  /**
   * Consumes <code>offset - position</code> elements from the {@link ResultSet}.
   *
   * @param offset The offset to advance to
   * @return True if the resultSet was advanced to the current point, false if insufficient rows
   *      were present to advance to the requested offset.
   */
  public boolean advanceResultSetToOffset(ResultSet results, long offset) throws SQLException {
    if (offset < 0 || offset < position) {
      throw new IllegalArgumentException("Offset should be "
        + " non-negative and not less than the current position. " + offset + ", " + position);
    }
    if (position >= offset) {
      return true;
    }

    if (null == relativeSupported) {
      Boolean moreResults = null;
      synchronized (this) {
        if (null == relativeSupported) {
          try {
            moreResults = advanceByRelative(results, offset);
            relativeSupported = true;
          } catch (SQLFeatureNotSupportedException e) {
            relativeSupported = false;
          }
        }
      }

      if (null != moreResults) {
        // We figured out whether or not relative is supported.
        // Make sure we actually do the necessary work.
        if (!relativeSupported) {
          // We avoided calling advanceByNext in the synchronized block earlier.
          moreResults = advanceByNext(results, offset);
        }

        return moreResults;
      }

      // Another thread updated the RELATIVE_SUPPORTED before we did, fall through.
    }

    if (relativeSupported) {
      return advanceByRelative(results, offset);
    } else {
      return advanceByNext(results, offset);
    }
  }

  private boolean advanceByRelative(ResultSet results, long offset) throws SQLException {
    long diff = offset - position;
    while (diff > Integer.MAX_VALUE) {
      if (!results.relative(Integer.MAX_VALUE)) {
        // Avoid updating position until relative succeeds.
        position += Integer.MAX_VALUE;
        return false;
      }
      // Avoid updating position until relative succeeds.
      position += Integer.MAX_VALUE;
      diff -= Integer.MAX_VALUE;
    }
    boolean ret = results.relative((int) diff);
    // Make sure we only update the position after successfully calling relative(int).
    position += diff;
    return ret;
  }

  private boolean advanceByNext(ResultSet results, long offset) throws SQLException {
    while (position < offset) {
      // Advance while maintaining `position`
      if (!_next(results)) {
        return false;
      }
    }

    return true;
  }
}

// End StatementInfo.java
