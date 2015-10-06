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
package org.apache.calcite.avatica.util;

import org.apache.calcite.avatica.ColumnMetaData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Implementation of JDBC {@link Struct}. */
public class StructImpl implements Struct {
  private final List list;

  public StructImpl(List list) {
    this.list = list;
  }

  @Override public String toString() {
    final Iterator iterator = list.iterator();
    if (!iterator.hasNext()) {
      return "{}";
    }
    final StringBuilder buf = new StringBuilder("{");
    for (;;) {
      append(buf, iterator.next());
      if (!iterator.hasNext()) {
        return buf.append("}").toString();
      }
      buf.append(", ");
    }
  }

  @Override public String getSQLTypeName() throws SQLException {
    return "ROW";
  }

  @Override public Object[] getAttributes() throws SQLException {
    return list.toArray();
  }

  @Override public Object[] getAttributes(Map<String, Class<?>> map)
      throws SQLException {
    throw new UnsupportedOperationException(); // TODO
  }

  private void append(StringBuilder buf, Object o) {
    if (o == null) {
      buf.append("null");
    } else {
      buf.append(o);
    }
  }

  /** Factory that can create a result set based on a list of values. */
  public interface Factory {
    ResultSet create(ColumnMetaData.AvaticaType elementType,
        Iterable<Object> iterable);
  }
}

// End StructImpl.java
