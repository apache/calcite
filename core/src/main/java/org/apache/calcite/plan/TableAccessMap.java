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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO jvs 9-Mar-2006:  move this class to another package; it
// doesn't really belong here.  Also, use a proper class for table
// names instead of List<String>.

/**
 * <code>TableAccessMap</code> represents the tables accessed by a query plan,
 * with READ/WRITE information.
 */
public class TableAccessMap {
  //~ Enums ------------------------------------------------------------------

  /** Access mode. */
  public enum Mode {
    /**
     * Table is not accessed at all.
     */
    NO_ACCESS,

    /**
     * Table is accessed for read only.
     */
    READ_ACCESS,

    /**
     * Table is accessed for write only.
     */
    WRITE_ACCESS,

    /**
     * Table is accessed for both read and write.
     */
    READWRITE_ACCESS
  }

  //~ Instance fields --------------------------------------------------------

  private final Map<List<String>, Mode> accessMap;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a permanently empty TableAccessMap.
   */
  public TableAccessMap() {
    accessMap = Collections.EMPTY_MAP;
  }

  /**
   * Constructs a TableAccessMap for all tables accessed by a RelNode and its
   * descendants.
   *
   * @param rel the RelNode for which to build the map
   */
  public TableAccessMap(RelNode rel) {
    // NOTE jvs 9-Mar-2006: This method must NOT retain a reference to the
    // input rel, because we use it for cached statements, and we don't
    // want to retain any rel references after preparation completes.

    accessMap = new HashMap<>();
    RelOptUtil.go(
        new TableRelVisitor(),
        rel);
  }

  /**
   * Constructs a TableAccessMap for a single table
   *
   * @param table fully qualified name of the table, represented as a list
   * @param mode  access mode for the table
   */
  public TableAccessMap(List<String> table, Mode mode) {
    accessMap = new HashMap<>();
    accessMap.put(table, mode);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return set of qualified names for all tables accessed
   */
  public Set<List<String>> getTablesAccessed() {
    return accessMap.keySet();
  }

  /**
   * Determines whether a table is accessed at all.
   *
   * @param tableName qualified name of the table of interest
   * @return true if table is accessed
   */
  public boolean isTableAccessed(List<String> tableName) {
    return accessMap.containsKey(tableName);
  }

  /**
   * Determines whether a table is accessed for read.
   *
   * @param tableName qualified name of the table of interest
   * @return true if table is accessed for read
   */
  public boolean isTableAccessedForRead(List<String> tableName) {
    Mode mode = getTableAccessMode(tableName);
    return (mode == Mode.READ_ACCESS) || (mode == Mode.READWRITE_ACCESS);
  }

  /**
   * Determines whether a table is accessed for write.
   *
   * @param tableName qualified name of the table of interest
   * @return true if table is accessed for write
   */
  public boolean isTableAccessedForWrite(List<String> tableName) {
    Mode mode = getTableAccessMode(tableName);
    return (mode == Mode.WRITE_ACCESS) || (mode == Mode.READWRITE_ACCESS);
  }

  /**
   * Determines the access mode of a table.
   *
   * @param tableName qualified name of the table of interest
   * @return access mode
   */
  public Mode getTableAccessMode(List<String> tableName) {
    Mode mode = accessMap.get(tableName);
    if (mode == null) {
      return Mode.NO_ACCESS;
    }
    return mode;
  }

  /**
   * Constructs a qualified name for an optimizer table reference.
   *
   * @param table table of interest
   * @return qualified name
   */
  public List<String> getQualifiedName(RelOptTable table) {
    return table.getQualifiedName();
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Visitor that finds all tables in a tree. */
  private class TableRelVisitor extends RelVisitor {
    public void visit(
        RelNode p,
        int ordinal,
        RelNode parent) {
      super.visit(p, ordinal, parent);
      RelOptTable table = p.getTable();
      if (table == null) {
        return;
      }
      Mode newAccess;

      // FIXME jvs 1-Feb-2006:  Don't rely on object type here;
      // eventually someone is going to write a rule which transforms
      // to something which doesn't inherit TableModify,
      // and this will break.  Need to make this explicit in
      // the RelNode interface.
      if (p instanceof TableModify) {
        newAccess = Mode.WRITE_ACCESS;
      } else {
        newAccess = Mode.READ_ACCESS;
      }
      List<String> key = getQualifiedName(table);
      Mode oldAccess = accessMap.get(key);
      if ((oldAccess != null) && (oldAccess != newAccess)) {
        newAccess = Mode.READWRITE_ACCESS;
      }
      accessMap.put(key, newAccess);
    }
  }
}

// End TableAccessMap.java
