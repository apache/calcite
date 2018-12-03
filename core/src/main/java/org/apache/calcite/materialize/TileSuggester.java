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
package org.apache.calcite.materialize;

import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.pentaho.aggdes.algorithm.Algorithm;
import org.pentaho.aggdes.algorithm.Progress;
import org.pentaho.aggdes.algorithm.Result;
import org.pentaho.aggdes.algorithm.impl.MonteCarloAlgorithm;
import org.pentaho.aggdes.algorithm.util.ArgumentUtils;
import org.pentaho.aggdes.model.Aggregate;
import org.pentaho.aggdes.model.Attribute;
import org.pentaho.aggdes.model.Dialect;
import org.pentaho.aggdes.model.Dimension;
import org.pentaho.aggdes.model.Measure;
import org.pentaho.aggdes.model.Parameter;
import org.pentaho.aggdes.model.Schema;
import org.pentaho.aggdes.model.StatisticsProvider;
import org.pentaho.aggdes.model.Table;

import java.io.PrintWriter;
import java.util.List;

/**
 * Algorithm that suggests a set of initial tiles (materialized aggregate views)
 * for a given lattice.
 */
public class TileSuggester {
  private final Lattice lattice;

  public TileSuggester(Lattice lattice) {
    this.lattice = lattice;
  }

  public Iterable<? extends Lattice.Tile> tiles() {
    final Algorithm algorithm = new MonteCarloAlgorithm();
    final PrintWriter pw = Util.printWriter(System.out);
    final Progress progress = new ArgumentUtils.TextProgress(pw);
    final StatisticsProvider statisticsProvider =
        new StatisticsProviderImpl(lattice);
    final double f = statisticsProvider.getFactRowCount();
    final ImmutableMap.Builder<Parameter, Object> map = ImmutableMap.builder();
    if (lattice.algorithmMaxMillis >= 0) {
      map.put(Algorithm.ParameterEnum.timeLimitSeconds,
          Math.max(1, (int) (lattice.algorithmMaxMillis / 1000L)));
    }
    map.put(Algorithm.ParameterEnum.aggregateLimit, 3);
    map.put(Algorithm.ParameterEnum.costLimit, f * 5d);
    final SchemaImpl schema = new SchemaImpl(lattice, statisticsProvider);
    final Result result = algorithm.run(schema, map.build(), progress);
    final ImmutableList.Builder<Lattice.Tile> tiles = ImmutableList.builder();
    for (Aggregate aggregate : result.getAggregates()) {
      tiles.add(toTile(aggregate));
    }
    return tiles.build();
  }

  private Lattice.Tile toTile(Aggregate aggregate) {
    final Lattice.TileBuilder tileBuilder = new Lattice.TileBuilder();
    for (Lattice.Measure measure : lattice.defaultMeasures) {
      tileBuilder.addMeasure(measure);
    }
    for (Attribute attribute : aggregate.getAttributes()) {
      tileBuilder.addDimension(((AttributeImpl) attribute).column);
    }
    return tileBuilder.build();
  }

  /** Implementation of {@link Schema} based on a {@link Lattice}. */
  private static class SchemaImpl implements Schema {
    private final StatisticsProvider statisticsProvider;
    private final TableImpl table;
    private final ImmutableList<AttributeImpl> attributes;

    SchemaImpl(Lattice lattice, StatisticsProvider statisticsProvider) {
      this.statisticsProvider = statisticsProvider;
      this.table = new TableImpl();
      final ImmutableList.Builder<AttributeImpl> attributeBuilder =
          ImmutableList.builder();
      for (Lattice.Column column : lattice.columns) {
        attributeBuilder.add(new AttributeImpl(column, table));
      }
      this.attributes = attributeBuilder.build();
    }

    public List<? extends Table> getTables() {
      return ImmutableList.of(table);
    }

    public List<Measure> getMeasures() {
      throw new UnsupportedOperationException();
    }

    public List<? extends Dimension> getDimensions() {
      throw new UnsupportedOperationException();
    }

    public List<? extends Attribute> getAttributes() {
      return attributes;
    }

    public StatisticsProvider getStatisticsProvider() {
      return statisticsProvider;
    }

    public Dialect getDialect() {
      throw new UnsupportedOperationException();
    }

    public String generateAggregateSql(Aggregate aggregate,
        List<String> columnNameList) {
      throw new UnsupportedOperationException();
    }
  }

  /** Implementation of {@link Table} based on a {@link Lattice}.
   * There is only one table (in this sense of table) in a lattice.
   * The algorithm does not really care about tables. */
  private static class TableImpl implements Table {
    public String getLabel() {
      return "TABLE";
    }

    public Table getParent() {
      return null;
    }
  }

  /** Implementation of {@link Attribute} based on a {@link Lattice.Column}. */
  private static class AttributeImpl implements Attribute {
    private final Lattice.Column column;
    private final TableImpl table;

    private AttributeImpl(Lattice.Column column, TableImpl table) {
      this.column = column;
      this.table = table;
    }

    @Override public String toString() {
      return getLabel();
    }

    public String getLabel() {
      return column.alias;
    }

    public Table getTable() {
      return table;
    }

    public double estimateSpace() {
      return 0;
    }

    public String getCandidateColumnName() {
      return null;
    }

    public String getDatatype(Dialect dialect) {
      return null;
    }

    public List<Attribute> getAncestorAttributes() {
      return ImmutableList.of();
    }
  }

  /** Implementation of {@link org.pentaho.aggdes.model.StatisticsProvider}
   * that asks the lattice. */
  private static class StatisticsProviderImpl implements StatisticsProvider {
    private final Lattice lattice;

    StatisticsProviderImpl(Lattice lattice) {
      this.lattice = lattice;
    }

    public double getFactRowCount() {
      return lattice.getFactRowCount();
    }

    public double getRowCount(List<Attribute> attributes) {
      return lattice.getRowCount(
          Util.transform(attributes, input -> ((AttributeImpl) input).column));
    }

    public double getSpace(List<Attribute> attributes) {
      return attributes.size();
    }

    public double getLoadTime(List<Attribute> attributes) {
      return getSpace(attributes) * getRowCount(attributes);
    }
  }
}

// End TileSuggester.java
