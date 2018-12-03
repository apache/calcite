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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.apache.geode.cache.query.SelectResults;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.convertToRowValues;

/**
 * Enumerator that reads from a Geode Regions.
 */
class GeodeEnumerator implements Enumerator<Object> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(GeodeEnumerator.class.getName());

  private Iterator iterator;
  private Object current;
  private List<RelDataTypeField> fieldTypes;

  /**
   * Creates a GeodeEnumerator.
   *
   * @param results      Geode result set ({@link SelectResults})
   * @param protoRowType The type of resulting rows
   */
  GeodeEnumerator(SelectResults results, RelProtoDataType protoRowType) {
    if (results == null) {
      LOGGER.warn("Null OQL results!");
    }
    this.iterator = (results == null) ? Collections.emptyIterator() : results.iterator();
    this.current = null;

    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
  }

  /**
   * Produces the next row from the results.
   *
   * @return A rel row from the results
   */
  @Override public Object current() {
    return convertToRowValues(fieldTypes, current);
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      current = iterator.next();
      return true;
    } else {
      return false;
    }
  }

  @Override public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override public void close() {
    // Nothing to do here
  }
}

// End GeodeEnumerator.java
