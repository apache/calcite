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
package org.apache.calcite.adapter.gremlin;

import org.apache.calcite.adapter.gremlin.converter.SqlConverter;
import org.apache.calcite.adapter.gremlin.converter.schema.SqlSchemaGrabber;
import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinSchema;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinProperty;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.driver.BaseGroovyGremlinShellEnvironment;
import org.apache.calcite.adapter.gremlin.driver.ConfigureSupport;
import org.apache.calcite.adapter.gremlin.driver.GremlinRemoteDriver;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Util;

import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class GremlinTableFactory implements TableFactory<GremlinTableBase> {
  @Override public GremlinTableBase create(SchemaPlus schema, String name, Map<String, Object> operand, @Nullable RelDataType rowType)  {

    final String dataSource = (String) operand.get("dataSource");
    final String query = (String) operand.get("query");

    final String dataSourceName = Util.first(dataSource, name);

    Groovysh groovysh = new Groovysh();
    GremlinShellEnvironment env = new BaseGroovyGremlinShellEnvironment(groovysh);
    GremlinRemoteDriver acceptor = new GremlinRemoteDriver(env);
    try {
      //FIXME
      Object result = acceptor.connect(Arrays.asList(Storage.toPath(ConfigureSupport.generateTempFileFromResource(this.getClass(), "gremlin-server-integration.yaml", ".tmp")), "session-managed"));
      TinkerGraph graph = (TinkerGraph) result;
      GraphTraversalSource g = graph.traversal();
      final GremlinSchema gremlinSchema = SqlSchemaGrabber.getSchema(g, SqlSchemaGrabber.ScanType.All);
      SqlConverter converter = new SqlConverter(gremlinSchema, g);
      final SqlGremlinResult sqlResult = new SqlGremlinResult(converter.executeQuery(query));
      //FIXME Supplementary column information
      return new GremlinTableBase("", false,convert(sqlResult.getColumns()));
    }catch (IOException | RemoteException ex) {
      ex.printStackTrace();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
    throw new IllegalStateException("this graph is not exists!");

  }

  //TODO
  public Map<String, GremlinProperty> convert(List<String> column){
    return new HashMap<>();
  }
}
