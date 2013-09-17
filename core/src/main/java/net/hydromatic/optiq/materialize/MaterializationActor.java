/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.materialize;

import net.hydromatic.optiq.Schema;

import org.eigenbase.reltype.RelDataType;

import java.util.*;

/**
 * Actor that manages the state of materializations in the system.
 */
class MaterializationActor {
  // Not an actor yet -- TODO make members private and add request/response
  // queues

  final Map<MaterializationKey, Materialization> keyMap =
      new HashMap<MaterializationKey, Materialization>();

  static class Materialization {
    final MaterializationKey key;
    final Schema rootSchema;
    Schema.TableInSchema materializedTable;
    final String sql;
    final RelDataType rowType;

    /** Creates a materialization.
     *
     * @param key  Unique identifier of this materialization
     * @param materializedTable Table that currently materializes the query.
     *                          That is, executing "select * from table" will
     *                          give the same results as executing the query.
     *                          May be null when the materialization is created;
     *                          materialization service will change the value as
     * @param sql  Query that is materialized
     * @param rowType Row type
     */
    Materialization(MaterializationKey key,
        Schema rootSchema,
        Schema.TableInSchema materializedTable,
        String sql,
        RelDataType rowType) {
      this.key = key;
      this.rootSchema = rootSchema;
      this.materializedTable = materializedTable; // may be null
      this.sql = sql;
      this.rowType = rowType;
    }
  }
}

// End MaterializationActor.java
