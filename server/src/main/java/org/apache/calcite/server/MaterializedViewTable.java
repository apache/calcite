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
package org.apache.calcite.server;

import org.apache.calcite.materialize.MaterializationKey;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

/** A table that implements a materialized view. */
class MaterializedViewTable
    extends MutableArrayTable {
  /** The key with which this was stored in the materialization service,
   * or null if not (yet) materialized. */
  MaterializationKey key;

  MaterializedViewTable(String name, RelProtoDataType protoRowType) {
    super(name, protoRowType, protoRowType,
        NullInitializerExpressionFactory.INSTANCE);
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.MATERIALIZED_VIEW;
  }

  @Override public <C> C unwrap(Class<C> aClass) {
    if (MaterializationKey.class.isAssignableFrom(aClass)
        && aClass.isInstance(key)) {
      return aClass.cast(key);
    }
    return super.unwrap(aClass);
  }
}
