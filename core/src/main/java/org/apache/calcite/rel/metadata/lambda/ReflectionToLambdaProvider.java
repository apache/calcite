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
package org.apache.calcite.rel.metadata.lambda;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdAllPredicates;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdLowerBoundCost;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.common.primitives.Primitives;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Create a set of lambda handlers via reflection patterns.
 *
 * This does reflection to lambda conversion based on a set of singleton objects
 * with appropriate signatures, similar to how ReflectiveRelMetadataProvider works.
 * Any class that can be discovered using ReflectiveRelMetadataProvider should also be consumable
 * using this class. Ultimately, the goal may be to move to direct lambda registration as opposed to
 * the old system of partial reflection discovery.
 */
@ThreadSafe
public class ReflectionToLambdaProvider implements LambdaProvider {

  public static final ImmutableList<Source> DEFAULT_SOURCES = ImmutableList.<Source>builder()
      .add(
          s(RelMdColumnOrigins.class, BuiltInMethod.COLUMN_ORIGIN,
          MetadataLambda.ColumnOrigins.class))
      .add(
          s(RelMdPercentageOriginalRows.class, BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS,
          MetadataLambda.PercentageOriginalRows.class))
      .add(
          s(RelMdExpressionLineage.class, BuiltInMethod.EXPRESSION_LINEAGE,
          MetadataLambda.ExpressionLineage.class))
      .add(
          s(RelMdTableReferences.class, BuiltInMethod.TABLE_REFERENCES,
          MetadataLambda.TableReferences.class))
      .add(
          s(RelMdNodeTypes.class, BuiltInMethod.NODE_TYPES,
          MetadataLambda.NodeTypes.class))
      .add(
          s(RelMdRowCount.class, BuiltInMethod.ROW_COUNT,
          MetadataLambda.RowCount.class))
      .add(
          s(RelMdMaxRowCount.class, BuiltInMethod.MAX_ROW_COUNT,
          MetadataLambda.MaxRowCount.class))
      .add(
          s(RelMdMinRowCount.class, BuiltInMethod.MIN_ROW_COUNT,
          MetadataLambda.MinRowCount.class))
      .add(
          s(RelMdUniqueKeys.class, BuiltInMethod.UNIQUE_KEYS,
          MetadataLambda.UniqueKeys.class))
      .add(
          s(RelMdColumnUniqueness.class, BuiltInMethod.COLUMN_UNIQUENESS,
          MetadataLambda.ColumnsUnique.class))
      .add(
          s(RelMdPopulationSize.class, BuiltInMethod.POPULATION_SIZE,
          MetadataLambda.PopulationSize.class))
      .add(
          s(RelMdSize.class, BuiltInMethod.AVERAGE_ROW_SIZE,
          MetadataLambda.AverageRowSize.class))
      .add(
          s(RelMdSize.class, BuiltInMethod.AVERAGE_COLUMN_SIZES,
          MetadataLambda.AverageColumnSizes.class))
      .add(
          s(RelMdParallelism.class, BuiltInMethod.IS_PHASE_TRANSITION,
          MetadataLambda.PhaseTransition.class))
      .add(
          s(RelMdParallelism.class, BuiltInMethod.SPLIT_COUNT,
          MetadataLambda.SplitCount.class))
      .add(
          s(RelMdDistribution.class, BuiltInMethod.DISTRIBUTION,
          MetadataLambda.Distribution.class))
      .add(
          s(RelMdLowerBoundCost.class, BuiltInMethod.LOWER_BOUND_COST,
          MetadataLambda.LowerBoundCost.class))
      .add(
          s(RelMdMemory.class, BuiltInMethod.MEMORY,
          MetadataLambda.Memory.class))
      .add(
          s(RelMdDistinctRowCount.class, BuiltInMethod.DISTINCT_ROW_COUNT,
          MetadataLambda.DistinctRowCount.class))
      .add(
          s(RelMdSelectivity.class, BuiltInMethod.SELECTIVITY,
          MetadataLambda.Selectivity.class))
      .add(
          s(RelMdExplainVisibility.class, BuiltInMethod.EXPLAIN_VISIBILITY,
          MetadataLambda.VisibleInExplain.class))
      .add(
          s(RelMdPredicates.class, BuiltInMethod.PREDICATES,
          MetadataLambda.PulledUpPredicates.class))
      .add(
          s(RelMdAllPredicates.class, BuiltInMethod.ALL_PREDICATES,
          MetadataLambda.AllPredicates.class))
      .add(
          s(RelMdCollation.class, BuiltInMethod.COLLATIONS,
          MetadataLambda.Collations.class))
      .add(
          s(RelMdPercentageOriginalRows.class, BuiltInMethod.CUMULATIVE_COST,
          MetadataLambda.CumulativeCost.class))
      .add(
          s(RelMdPercentageOriginalRows.class, BuiltInMethod.NON_CUMULATIVE_COST,
          MetadataLambda.NonCumulativeCost.class))
      .build();

  // Maintains a list of lambdas associated with each RelNode + MetadataInterface pair.
  private final Table<Class<?>, Class<?>, List<Object>> items;

  // Maintains an ordered list of relnode interfaces for a particular relnode. This is done so we
  // don't have to do the enumeration for each separate MetadataLambda.
  private final LoadingCache<Class<? extends RelNode>, List<Class<? extends RelNode>>>
      classImplementations = CacheBuilder.newBuilder().build(
          new CacheLoader<Class<? extends RelNode>, List<Class<? extends RelNode>>>() {
            @Override public List<Class<? extends RelNode>> load(final Class<? extends RelNode> key)
                throws Exception {
              return getImplements((Class<? extends RelNode>) key);
            }
          });

  public ReflectionToLambdaProvider(Source... sources) {
    this(Arrays.asList(sources));
  }

  public ReflectionToLambdaProvider() {
    this(DEFAULT_SOURCES);
  }

  public ReflectionToLambdaProvider(Iterable<Source> sources) {

    // build a list of lambdas for a given class.
    HashBasedTable<Class<?>, Class<?>, List<Object>> table = HashBasedTable.create();

    for (Source source : sources) {
      Map<Class<?>, Object> lambdas = findMethodsAndCreateLambdas(source);
      for (Map.Entry<Class<?>, Object> e : lambdas.entrySet()) {
        List<Object> objects = table.get(e.getKey(), source.lambdaClass);
        if (objects == null) {
          objects = new ArrayList<>();
          table.put(e.getKey(), source.lambdaClass, objects);
        }
        objects.add(e.getValue());
      }
    }

    this.items = table;
  }

  /**
   * For a given source, generate a map of specific RelNode to MetadataLambda mappings.
   */
  private Map<Class<?>, Object> findMethodsAndCreateLambdas(Source source) {
    Class<?> clazz = source.singleton.getClass();
    String name = source.sourceMethod;
    Class<?> arg = source.lambdaClass;
    try {
      final Object singleton = source.singleton;

      List<Method> methods = Arrays.stream(clazz.getMethods())
          .filter(f -> f.getName().equals(name))
          .collect(Collectors.toList());
      Map<Class<?>, Object> output = new HashMap<>();
      for (Method reflectionMethod : methods) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType callSiteType = MethodType.methodType(arg, clazz);
        MethodType functionalMethod;
        MethodType delegateMethod;

        // generate methods based on number of arguments.
        if (MetadataLambda.Arg0Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class,
              reflectionMethod.getParameterTypes()[1]);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(),
              reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class);
        } else if (MetadataLambda.Arg1Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class,
              reflectionMethod.getParameterTypes()[1], Object.class);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(),
              reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class,
              reflectionMethod.getParameterTypes()[2]);
          if (reflectionMethod.getParameterTypes()[2].isPrimitive()) {
            delegateMethod = delegateMethod.changeParameterType(2,
                Primitives.wrap(reflectionMethod.getParameterTypes()[2]));
          }
        } else if (MetadataLambda.Arg2Lambda.class.isAssignableFrom(arg)) {
          functionalMethod = MethodType.methodType(Object.class, RelNode.class,
              reflectionMethod.getParameterTypes()[1], Object.class, Object.class);
          delegateMethod = MethodType.methodType(reflectionMethod.getReturnType(),
              reflectionMethod.getParameterTypes()[0], RelMetadataQuery.class,
              reflectionMethod.getParameterTypes()[2], reflectionMethod.getParameterTypes()[3]);
          if (reflectionMethod.getParameterTypes()[2].isPrimitive()) {
            delegateMethod = delegateMethod.changeParameterType(2,
                Primitives.wrap(reflectionMethod.getParameterTypes()[2]));
          }
          if (reflectionMethod.getParameterTypes()[3].isPrimitive()) {
            delegateMethod = delegateMethod.changeParameterType(3,
                Primitives.wrap(reflectionMethod.getParameterTypes()[3]));
          }
        } else {
          throw new IllegalStateException();
        }
        MethodHandle delegate = lookup.unreflect(reflectionMethod);
        CallSite callSite = LambdaMetafactory.metafactory(lookup, "call",
            callSiteType, functionalMethod, delegate, delegateMethod);
        Object val = callSite.getTarget().bindTo(singleton).invoke();
        output.put(reflectionMethod.getParameterTypes()[0], val);
      }
      if (output.isEmpty()) {
        throw new UnsupportedOperationException(
            String.format(Locale.ROOT, "Provided source has no methods found. Method Name: %s, "
                + "Singleton Type: %s, Lambda Class %s.", source.singleton.getClass().getName(),
                source.sourceMethod, source.lambdaClass.getSimpleName()));
      }
      return output;
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Describes a source of metadata methods. **/
  public static class Source {
    private final Object singleton;
    private final String sourceMethod;
    private final Class<?> lambdaClass;

    private Source(
        final Class<?> privateSingletonClass,
        final String sourceMethod,
        final Class<?> lambdaClass) {
      this(privateSingleton(privateSingletonClass), sourceMethod, lambdaClass);
    }

    private Source(final Object singleton, final String sourceMethod, final Class<?> lambdaClass) {
      this.singleton = singleton;
      this.sourceMethod = sourceMethod;
      this.lambdaClass = lambdaClass;
    }

    private static Object privateSingleton(Class<?> clazz) {
      try {
        final Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        Constructor<?> noArg = Arrays.stream(constructors)
            .filter(c -> c.getParameterTypes().length == 0).findFirst().get();
        noArg.setAccessible(true);
        return noArg.newInstance();
      } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
        throw new RuntimeException(e);
      }
    }

    public static Source of(Class<?> sourceClass, String sourceMethod, Class<?> lambdaClass) {
      return new Source(sourceClass, sourceMethod, lambdaClass);
    }

    public static Source of(Object instance, String sourceMethod, Class<?> lambdaClass) {
      return new Source(instance, sourceMethod, lambdaClass);
    }
  }

  @Override public <R extends RelNode, T extends MetadataLambda> List<T> get(
      Class<R> relnodeClass,
      Class<T> handlerClass) throws ExecutionException {
    List<Class<? extends RelNode>> classes = classImplementations.get(relnodeClass);
    ImmutableList.Builder<T> handlers = ImmutableList.builder();
    for (Class<?> clazz : classes) {
      List<Object> partialHandlers = items.get(clazz, handlerClass);
      if (partialHandlers == null) {
        continue;
      }

      for (Object o : partialHandlers) {
        handlers.add((T) o);
      }
    }
    return handlers.build();
  }

  private static Source s(Class<?> sourceClass, String sourceMethod, Class<?> lambdaClass) {
    return Source.of(sourceClass, sourceMethod, lambdaClass);
  }

  private static Source s(Class<?> sourceClass, BuiltInMethod method, Class<?> lambdaClass) {
    return Source.of(sourceClass, method.getMethodName(), lambdaClass);
  }

  private static Source s(Object instance, String sourceMethod, Class<?> lambdaClass) {
    return Source.of(instance, sourceMethod, lambdaClass);
  }

  /**
   * Generate a list of interfaces/classes that this node implements, from nearest to furthest.
   */
  private static List<Class<? extends RelNode>> getImplements(Class<? extends RelNode> base) {
    ImmutableList.Builder<Class<? extends RelNode>> builder = ImmutableList.builder();
    addImplements(base, builder);
    return builder.build();
  }

  private static void addImplements(
      Class<?> base,
      ImmutableList.Builder<Class<? extends RelNode>> builder) {
    if (base == null || !RelNode.class.isAssignableFrom(base)) {
      return;
    }
    builder.add((Class<? extends RelNode>) base);
    Arrays.stream(base.getInterfaces()).forEach(c -> addImplements(c, builder));
    addImplements(base.getSuperclass(), builder);
  }

}
