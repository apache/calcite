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
package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GremlinEdgeTable extends GremlinTableBase {
    private final List<Pair<String, String>> inOutVertexPairs;

    public GremlinEdgeTable(final String label, final List<GremlinProperty> columns,
                            final List<Pair<String, String>> inOutVertexPairs) {
        super(label, false, convert(label, columns, inOutVertexPairs));
        this.inOutVertexPairs = inOutVertexPairs;
    }

  public List<Pair<String, String>> getInOutVertexPairs() {
    return inOutVertexPairs;
  }

  private static Map<String, GremlinProperty> convert(
            final String label, final List<GremlinProperty> columns,
            final List<Pair<String, String>> inOutTablePairs) {
        final Map<String, GremlinProperty> columnsWithPKFK =
                columns.stream().collect(Collectors.toMap(GremlinProperty::getName, t -> t));

        // Uppercase edge label appended with '_ID' represents an edge, this is a string type.
        final GremlinProperty pk = new GremlinProperty(label + GremlinTableBase.ID, "string");
        columnsWithPKFK.put(pk.getName(), pk);

        // Get in and out foreign keys of edge.
        inOutTablePairs.forEach(inOutPair -> {
            // Uppercase vertex label appended with 'IN_ID'/'OUT_ID' represents a connected vertex, this is a string type.
            final GremlinProperty inFk = new GremlinProperty(inOutPair.getKey() + GremlinTableBase.IN_ID, "string");
            final GremlinProperty outFk = new GremlinProperty(inOutPair.getValue() + GremlinTableBase.OUT_ID, "string");
            columnsWithPKFK.put(inFk.getName(), inFk);
            columnsWithPKFK.put(outFk.getName(), inFk);
        });
        return columnsWithPKFK;
    }

    public boolean isEdgeBetween(final String in, final String out) {
        for (final Pair<String, String> inOutPair : inOutVertexPairs) {
            if (inOutPair.getKey().equalsIgnoreCase(in + GremlinTableBase.IN_ID)
                    && inOutPair.getValue().equalsIgnoreCase(out + GremlinTableBase.OUT_ID)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasInVertex(final String inVertexLabel) {
        final String label = inVertexLabel.replace(IN_ID, "");
        for (final Pair<String, String> pair : inOutVertexPairs) {
            if (pair.getKey().equalsIgnoreCase(label)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasOutVertex(final String outVertexLabel) {
        final String label = outVertexLabel.replace(IN_ID, "");
        for (final Pair<String, String> pair : inOutVertexPairs) {
            if (pair.getValue().equalsIgnoreCase(label)) {
                return true;
            }
        }
        return false;
    }
}
