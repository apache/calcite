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
package org.apache.calcite.adapter.geode.simple;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.geode.cache.client.ClientCache;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.convertToRowValues;

/**
 * Geode Simple Scannable Table Abstraction
 */
public class GeodeSimpleScannableTable extends AbstractTable implements ScannableTable {

  private final RelDataType relDataType;
  private String regionName;
  private ClientCache clientCache;

  public GeodeSimpleScannableTable(String regionName, RelDataType relDataType,
      ClientCache clientCache) {
    super();

    this.regionName = regionName;
    this.clientCache = clientCache;
    this.relDataType = relDataType;
  }

  @Override public String toString() {
    return "GeodeSimpleScannableTable";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return relDataType;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new GeodeSimpleEnumerator<Object[]>(clientCache, regionName) {
          @Override public Object[] convert(Object obj) {
            Object values = convertToRowValues(relDataType.getFieldList(), obj);
            if (values instanceof Object[]) {
              return (Object[]) values;
            }
            return new Object[]{values};
          }
        };
      }
    };
  }
}

// End GeodeSimpleScannableTable.java
