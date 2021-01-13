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
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Definition of the convention trait.
 * A new set of conversion information is created for
 * each planner that registers at least one {@link ConverterRule} instance.
 *
 * <p>Conversion data is held in a {@link LoadingCache}
 * with weak keys so that the JVM's garbage
 * collector may reclaim the conversion data after the planner itself has been
 * garbage collected. The conversion information consists of a graph of
 * conversions (from one calling convention to another) and a map of graph arcs
 * to {@link ConverterRule}s.
 */
public class ConventionTraitDef extends RelTraitDef<Convention> {
  //~ Static fields/initializers ---------------------------------------------

  public static final ConventionTraitDef INSTANCE =
      new ConventionTraitDef();

  //~ Instance fields --------------------------------------------------------

  /**
   * Weak-key cache of RelOptPlanner to ConversionData. The idea is that when
   * the planner goes away, so does the cache entry.
   */
  private final LoadingCache<RelOptPlanner, ConversionData> conversionCache =
      CacheBuilder.newBuilder().weakKeys()
          .build(CacheLoader.from(ConversionData::new));

  //~ Constructors -----------------------------------------------------------

  private ConventionTraitDef() {
    super();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelTraitDef
  @Override public Class<Convention> getTraitClass() {
    return Convention.class;
  }

  @Override public String getSimpleName() {
    return "convention";
  }

  @Override public Convention getDefault() {
    return Convention.NONE;
  }

  @Override public void registerConverterRule(
      RelOptPlanner planner,
      ConverterRule converterRule) {
    if (converterRule.isGuaranteed()) {
      ConversionData conversionData = getConversionData(planner);

      final Convention inConvention =
          (Convention) converterRule.getInTrait();
      final Convention outConvention =
          (Convention) converterRule.getOutTrait();
      conversionData.conversionGraph.addVertex(inConvention);
      conversionData.conversionGraph.addVertex(outConvention);
      conversionData.conversionGraph.addEdge(inConvention, outConvention);

      conversionData.mapArcToConverterRule.put(
          Pair.of(inConvention, outConvention), converterRule);
    }
  }

  @Override public void deregisterConverterRule(
      RelOptPlanner planner,
      ConverterRule converterRule) {
    if (converterRule.isGuaranteed()) {
      ConversionData conversionData = getConversionData(planner);

      final Convention inConvention =
          (Convention) converterRule.getInTrait();
      final Convention outConvention =
          (Convention) converterRule.getOutTrait();

      final boolean removed =
          conversionData.conversionGraph.removeEdge(
              inConvention, outConvention);
      assert removed;
      conversionData.mapArcToConverterRule.remove(
          Pair.of(inConvention, outConvention), converterRule);
    }
  }

  // implement RelTraitDef
  @Override public @Nullable RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      Convention toConvention,
      boolean allowInfiniteCostConverters) {
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final ConversionData conversionData = getConversionData(planner);

    final Convention fromConvention = requireNonNull(rel.getConvention(),
        () -> "convention is null for rel " + rel);

    List<List<Convention>> conversionPaths =
        conversionData.getPaths(fromConvention, toConvention);

  loop:
    for (List<Convention> conversionPath : conversionPaths) {
      assert conversionPath.get(0) == fromConvention;
      assert conversionPath.get(conversionPath.size() - 1)
          == toConvention;
      RelNode converted = rel;
      Convention previous = null;
      for (Convention arc : conversionPath) {
        RelOptCost cost = planner.getCost(converted, mq);
        if ((cost == null || cost.isInfinite())
            && !allowInfiniteCostConverters) {
          continue loop;
        }
        if (previous != null) {
          converted =
              changeConvention(
                  converted, previous, arc,
                  conversionData.mapArcToConverterRule);
          if (converted == null) {
            throw new AssertionError("Converter from " + previous + " to " + arc
                + " guaranteed that it could convert any relexp");
          }
        }
        previous = arc;
      }
      return converted;
    }

    return null;
  }

  /**
   * Tries to convert a relational expression to the target convention of an
   * arc.
   */
  private static @Nullable RelNode changeConvention(
      RelNode rel,
      Convention source,
      Convention target,
      final Multimap<Pair<Convention, Convention>, ConverterRule>
          mapArcToConverterRule) {
    assert source == rel.getConvention();

    // Try to apply each converter rule for this arc's source/target calling
    // conventions.
    final Pair<Convention, Convention> key = Pair.of(source, target);
    for (ConverterRule rule : mapArcToConverterRule.get(key)) {
      assert rule.getInTrait() == source;
      assert rule.getOutTrait() == target;
      RelNode converted = rule.convert(rel);
      if (converted != null) {
        return converted;
      }
    }
    return null;
  }

  @Override public boolean canConvert(
      RelOptPlanner planner,
      Convention fromConvention,
      Convention toConvention) {
    ConversionData conversionData = getConversionData(planner);
    return fromConvention.canConvertConvention(toConvention)
        || conversionData.getShortestDistance(fromConvention, toConvention) != -1;
  }

  private ConversionData getConversionData(RelOptPlanner planner) {
    return conversionCache.getUnchecked(planner);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Workspace for converting from one convention to another. */
  private static final class ConversionData {
    final DirectedGraph<Convention, DefaultEdge> conversionGraph =
        DefaultDirectedGraph.create();

    /**
     * For a given source/target convention, there may be several possible
     * conversion rules. Maps {@link DefaultEdge} to a
     * collection of {@link ConverterRule} objects.
     */
    final Multimap<Pair<Convention, Convention>, ConverterRule> mapArcToConverterRule =
        HashMultimap.create();

    private Graphs.@MonotonicNonNull FrozenGraph<Convention, DefaultEdge> pathMap;

    public List<List<Convention>> getPaths(
        Convention fromConvention,
        Convention toConvention) {
      return getPathMap().getPaths(fromConvention, toConvention);
    }

    private Graphs.FrozenGraph<Convention, DefaultEdge> getPathMap() {
      if (pathMap == null) {
        pathMap = Graphs.makeImmutable(conversionGraph);
      }
      return pathMap;
    }

    public int getShortestDistance(
        Convention fromConvention,
        Convention toConvention) {
      return getPathMap().getShortestDistance(fromConvention, toConvention);
    }
  }
}
