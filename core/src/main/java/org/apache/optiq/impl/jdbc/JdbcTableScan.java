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
package org.apache.optiq.impl.jdbc;

import org.apache.optiq.rel.RelNode;
import org.apache.optiq.rel.TableAccessRelBase;
import org.apache.optiq.relopt.*;

import java.util.Collections;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcTableScan extends TableAccessRelBase implements JdbcRel {
  final JdbcTable jdbcTable;

  protected JdbcTableScan(
      RelOptCluster cluster,
      RelOptTable table,
      JdbcTable jdbcTable,
      JdbcConvention jdbcConvention) {
    super(cluster, cluster.traitSetOf(jdbcConvention), table);
    this.jdbcTable = jdbcTable;
    assert jdbcTable != null;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new JdbcTableScan(
        getCluster(), table, jdbcTable, (JdbcConvention) getConvention());
  }

  public JdbcImplementor.Result implement(JdbcImplementor implementor) {
    return implementor.result(jdbcTable.tableName(),
        Collections.singletonList(JdbcImplementor.Clause.FROM), this);
  }
}

// End JdbcTableScan.java
