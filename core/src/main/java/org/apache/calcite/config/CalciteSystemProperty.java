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
package org.apache.calcite.config;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A Calcite specific system property that is used to configure various aspects of the framework.
 *
 * <p>Calcite system properties must always be in the "calcite" root namespace.</p>
 *
 * @param <T> the type of the property value
 */
public final class CalciteSystemProperty<T> {
  /**
   * Holds all system properties related with the Calcite.
   *
   * <p>Deprecated <code>"saffron.properties"</code> (in namespaces"saffron" and "net.sf.saffron")
   * are also kept here but under "calcite" namespace.</p>
   */
  private static final Properties PROPERTIES = loadProperties();

  /**
   * Whether to run Calcite in debug mode.
   *
   * <p>When debug mode is activated significantly more information is gathered and printed to
   * STDOUT. It is most commonly used to print and identify problems in generated java code. Debug
   * mode is also used to perform more verifications at runtime, which are not performed during
   * normal execution.</p>
   */
  public static final CalciteSystemProperty<Boolean> DEBUG =
      booleanProperty("calcite.debug", false);

  /**
   * Whether to exploit join commutative property.
   */
  // TODO review zabetak:
  // Does the property control join commutativity or rather join associativity? The property is
  // associated with {@link org.apache.calcite.rel.rules.JoinAssociateRule} and not with
  // {@link org.apache.calcite.rel.rules.JoinCommuteRule}.
  public static final CalciteSystemProperty<Boolean> COMMUTE =
      booleanProperty("calcite.enable.join.commute", false);

  /** Whether to enable the collation trait in the default planner configuration.
   *
   * <p>Some extra optimizations are possible if enabled, but queries should
   * work either way. At some point this will become a preference, or we will
   * run multiple phases: first disabled, then enabled. */
  public static final CalciteSystemProperty<Boolean> ENABLE_COLLATION_TRAIT =
      booleanProperty("calcite.enable.collation.trait", true);

  /** Whether the enumerable convention is enabled in the default planner configuration. */
  public static final CalciteSystemProperty<Boolean> ENABLE_ENUMERABLE =
      booleanProperty("calcite.enable.enumerable", true);

  /** Whether the EnumerableTableScan should support ARRAY fields. */
  public static final CalciteSystemProperty<Boolean> ENUMERABLE_ENABLE_TABLESCAN_ARRAY =
      booleanProperty("calcite.enable.enumerable.tablescan.array", false);

  /** Whether the EnumerableTableScan should support MAP fields. */
  public static final CalciteSystemProperty<Boolean> ENUMERABLE_ENABLE_TABLESCAN_MAP =
      booleanProperty("calcite.enable.enumerable.tablescan.map", false);

  /** Whether the EnumerableTableScan should support MULTISET fields. */
  public static final CalciteSystemProperty<Boolean> ENUMERABLE_ENABLE_TABLESCAN_MULTISET =
      booleanProperty("calcite.enable.enumerable.tablescan.multiset", false);

  /** Whether streaming is enabled in the default planner configuration. */
  public static final CalciteSystemProperty<Boolean> ENABLE_STREAM =
      booleanProperty("calcite.enable.stream", true);

  /**
   * Whether RexNode digest should be normalized (e.g. call operands ordered).
   * <p>Normalization helps to treat $0=$1 and $1=$0 expressions equal, thus it saves efforts
   * on planning.</p> */
  public static final CalciteSystemProperty<Boolean> ENABLE_REX_DIGEST_NORMALIZE =
      booleanProperty("calcite.enable.rexnode.digest.normalize", true);

  /**
   *  Whether to follow the SQL standard strictly.
   */
  public static final CalciteSystemProperty<Boolean> STRICT =
      booleanProperty("calcite.strict.sql", false);

  /**
   * Whether to include a GraphViz representation when dumping the state of the Volcano planner.
   */
  public static final CalciteSystemProperty<Boolean> DUMP_GRAPHVIZ =
      booleanProperty("calcite.volcano.dump.graphviz", true);

  /**
   * Whether to include <code>RelSet</code> information when dumping the state of the Volcano
   * planner.
   */
  public static final CalciteSystemProperty<Boolean> DUMP_SETS =
      booleanProperty("calcite.volcano.dump.sets", true);

  /**
   * Whether to enable top-down optimization. This config can be overridden
   * by {@link CalciteConnectionProperty#TOPDOWN_OPT}.
   *
   * <p>Note: Enabling top-down optimization will automatically disable
   * the use of AbstractConverter and related rules.</p>
   */
  public static final CalciteSystemProperty<Boolean> TOPDOWN_OPT =
      booleanProperty("calcite.planner.topdown.opt", false);

  /**
   * Whether to run integration tests.
   */
  // TODO review zabetak:
  // The property is used in only one place and it is associated with mongodb. Should we drop this
  // property and just use TEST_MONGODB?
  public static final CalciteSystemProperty<Boolean> INTEGRATION_TEST =
      booleanProperty("calcite.integrationTest", false);

  /**
   * Which database to use for tests that require a JDBC data source.
   *
   * <p>The property can take one of the following values:
   *
   * <ul>
   *   <li>HSQLDB (default)</li>
   *   <li>H2</li>
   *   <li>MYSQL</li>
   *   <li>ORACLE</li>
   *   <li>POSTGRESQL</li>
   * </ul>
   *
   * <p>If the specified value is not included in the previous list, the default
   * is used.
   *
   * <p>We recommend that casual users use hsqldb, and frequent Calcite
   * developers use MySQL. The test suite runs faster against the MySQL database
   * (mainly because of the 0.1 second versus 6 seconds startup time). You have
   * to populate MySQL manually with the foodmart data set, otherwise there will
   * be test failures.
   */
  public static final CalciteSystemProperty<String> TEST_DB =
      stringProperty("calcite.test.db", "HSQLDB",
          ImmutableSet.of(
              "HSQLDB",
              "H2",
              "MYSQL",
              "ORACLE",
              "POSTGRESQL"));

  /**
   * Path to the dataset file that should used for integration tests.
   *
   * <p>If a path is not set, then one of the following values will be used:
   *
   * <ul>
   *   <li>../calcite-test-dataset</li>
   *   <li>../../calcite-test-dataset</li>
   *   <li>.</li>
   * </ul>
   * The first valid path that exists in the filesystem will be chosen.
   */
  public static final CalciteSystemProperty<String> TEST_DATASET_PATH =
      new CalciteSystemProperty<>("calcite.test.dataset", v -> {
        if (v != null) {
          return v;
        }
        final String[] dirs = {
            "../calcite-test-dataset",
            "../../calcite-test-dataset"
        };
        for (String s : dirs) {
          if (new File(s).exists() && new File(s, "vm").exists()) {
            return s;
          }
        }
        return ".";
      });

  /**
   * Whether to run MongoDB tests.
   */
  public static final CalciteSystemProperty<Boolean> TEST_MONGODB =
      booleanProperty("calcite.test.mongodb", true);

  /**
   * Whether to run Splunk tests.
   *
   * <p>Disabled by default, because we do not expect Splunk to be installed
   * and populated with the data set necessary for testing.
   */
  public static final CalciteSystemProperty<Boolean> TEST_SPLUNK =
      booleanProperty("calcite.test.splunk", false);

  /**
   * Whether to run Druid tests.
   */
  public static final CalciteSystemProperty<Boolean> TEST_DRUID =
      booleanProperty("calcite.test.druid", false);

  /**
   * Whether to run Cassandra tests.
   */
  public static final CalciteSystemProperty<Boolean> TEST_CASSANDRA =
      booleanProperty("calcite.test.cassandra", true);

  /**
   * Whether to run InnoDB tests.
   */
  public static final CalciteSystemProperty<Boolean> TEST_INNODB =
      booleanProperty("calcite.test.innodb", true);

  /**
   * Whether to run Redis tests.
   */
  public static final CalciteSystemProperty<Boolean> TEST_REDIS =
      booleanProperty("calcite.test.redis", true);

  /**
   * Whether to use Docker containers (https://www.testcontainers.org/) in tests.
   *
   * If the property is set to <code>true</code>, affected tests will attempt to start Docker
   * containers; when Docker is not available tests fallback to other execution modes and if it's
   * not possible they are skipped entirely.
   *
   * If the property is set to <code>false</code>, Docker containers are not used at all and
   * affected tests either fallback to other execution modes or skipped entirely.
   *
   * Users can override the default behavior to force non-Dockerized execution even when Docker
   * is installed on the machine; this can be useful for replicating an issue that appears only in
   * non-docker test mode or for running tests both with and without containers in CI.
   */
  public static final CalciteSystemProperty<Boolean> TEST_WITH_DOCKER_CONTAINER =
      booleanProperty("calcite.test.docker", true);

  /**
   * A list of ids designating the queries
   * (from query.json in new.hydromatic:foodmart-queries:0.4.1)
   * that should be run as part of FoodmartTest.
   */
  // TODO review zabetak:
  // The name of the property is not appropriate. A better alternative would be
  // calcite.test.foodmart.queries.ids. Moreover, I am not in favor of using system properties for
  // parameterized tests.
  public static final CalciteSystemProperty<@Nullable String> TEST_FOODMART_QUERY_IDS =
      new CalciteSystemProperty<>("calcite.ids", Function.<@Nullable String>identity());

  /**
   * Whether the optimizer will consider adding converters of infinite cost in
   * order to convert a relational expression from one calling convention to
   * another.
   */
  public static final CalciteSystemProperty<Boolean> ALLOW_INFINITE_COST_CONVERTERS =
      booleanProperty("calcite.opt.allowInfiniteCostConverters", true);

  /**
   * The name of the default character set.
   *
   * <p>It is used by {@link org.apache.calcite.sql.validate.SqlValidator}.
   */
  // TODO review zabetak:
  // What happens if a wrong value is specified?
  public static final CalciteSystemProperty<String> DEFAULT_CHARSET =
      stringProperty("calcite.default.charset", "ISO-8859-1");

  /**
   * The name of the default national character set.
   *
   * <p>It is used with the N'string' construct in
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}
   * and may be different from the {@link #DEFAULT_CHARSET}.
   */
  // TODO review zabetak:
  // What happens if a wrong value is specified?
  public static final CalciteSystemProperty<String> DEFAULT_NATIONAL_CHARSET =
      stringProperty("calcite.default.nationalcharset", "ISO-8859-1");

  /**
   * The name of the default collation.
   *
   * <p>It is used in {@link org.apache.calcite.sql.SqlCollation} and
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}.
   */
  // TODO review zabetak:
  // What happens if a wrong value is specified?
  public static final CalciteSystemProperty<String> DEFAULT_COLLATION =
      stringProperty("calcite.default.collation.name", "ISO-8859-1$en_US");

  /**
   * The strength of the default collation.
   * Allowed values (as defined in {@link java.text.Collator}) are: primary, secondary,
   * tertiary, identical.
   *
   * <p>It is used in {@link org.apache.calcite.sql.SqlCollation} and
   * {@link org.apache.calcite.sql.SqlLiteral#SqlLiteral}.</p>
   */
  // TODO review zabetak:
  // What happens if a wrong value is specified?
  public static final CalciteSystemProperty<String> DEFAULT_COLLATION_STRENGTH =
      stringProperty("calcite.default.collation.strength", "primary");

  /**
   * The maximum size of the cache of metadata handlers.
   *
   * <p>A typical value is the number of queries being concurrently prepared multiplied by the
   * number of types of metadata.</p>
   *
   * <p>If the value is less than 0, there is no limit.</p>
   */
  public static final CalciteSystemProperty<Integer> METADATA_HANDLER_CACHE_MAXIMUM_SIZE =
      intProperty("calcite.metadata.handler.cache.maximum.size", 1000);

  /**
   * The maximum size of the cache used for storing Bindable objects, instantiated via
   * dynamically generated Java classes.
   *
   * <p>The default value is 0.</p>
   *
   * <p>The property can take any value between [0, {@link Integer#MAX_VALUE}] inclusive. If the
   * value is not valid (or not specified) then the default value is used.</p>
   *
   * <p>The cached objects may be quite big so it is suggested to use a rather small cache size
   * (e.g., 1000). For the most common use cases a number close to 1000 should be enough to
   * alleviate the performance penalty of compiling and loading classes.</p>
   *
   * <p>Setting this property to 0 disables the cache.</p>
   */
  public static final CalciteSystemProperty<Integer> BINDABLE_CACHE_MAX_SIZE =
      intProperty("calcite.bindable.cache.maxSize", 0, v -> v >= 0 && v <= Integer.MAX_VALUE);

  /**
   * The concurrency level of the cache used for storing Bindable objects, instantiated via
   * dynamically generated Java classes.
   *
   * <p>The default value is 1.</p>
   *
   * <p>The property can take any value between [1, {@link Integer#MAX_VALUE}] inclusive. If the
   * value is not valid (or not specified) then the default value is used.</p>
   *
   * <p>This property has no effect if the cache is disabled (i.e., {@link #BINDABLE_CACHE_MAX_SIZE}
   * set to 0.</p>
   */
  public static final CalciteSystemProperty<Integer> BINDABLE_CACHE_CONCURRENCY_LEVEL =
      intProperty("calcite.bindable.cache.concurrencyLevel", 1,
          v -> v >= 1 && v <= Integer.MAX_VALUE);

  private static CalciteSystemProperty<Boolean> booleanProperty(String key,
      boolean defaultValue) {
    // Note that "" -> true (convenient for command-lines flags like '-Dflag')
    return new CalciteSystemProperty<>(key,
        v -> v == null ? defaultValue
            : "".equals(v) || Boolean.parseBoolean(v));
  }

  private static CalciteSystemProperty<Integer> intProperty(String key, int defaultValue) {
    return intProperty(key, defaultValue, v -> true);
  }

  /**
   * Returns the value of the system property with the specified name as {@code
   * int}. If any of the conditions below hold, returns the
   * <code>defaultValue</code>:
   *
   * <ol>
   * <li>the property is not defined;
   * <li>the property value cannot be transformed to an int;
   * <li>the property value does not satisfy the checker.
   * </ol>
   */
  private static CalciteSystemProperty<Integer> intProperty(String key, int defaultValue,
      IntPredicate valueChecker) {
    return new CalciteSystemProperty<>(key, v -> {
      if (v == null) {
        return defaultValue;
      }
      try {
        int intVal = Integer.parseInt(v);
        return valueChecker.test(intVal) ? intVal : defaultValue;
      } catch (NumberFormatException nfe) {
        return defaultValue;
      }
    });
  }

  private static CalciteSystemProperty<String> stringProperty(String key, String defaultValue) {
    return new CalciteSystemProperty<>(key, v -> v == null ? defaultValue : v);
  }

  private static CalciteSystemProperty<String> stringProperty(
      String key,
      String defaultValue,
      Set<String> allowedValues) {
    return new CalciteSystemProperty<>(key, v -> {
      if (v == null) {
        return defaultValue;
      }
      String normalizedValue = v.toUpperCase(Locale.ROOT);
      return allowedValues.contains(normalizedValue) ? normalizedValue : defaultValue;
    });
  }

  private static Properties loadProperties() {
    Properties saffronProperties = new Properties();
    ClassLoader classLoader = MoreObjects.firstNonNull(
        Thread.currentThread().getContextClassLoader(),
        CalciteSystemProperty.class.getClassLoader());
    // Read properties from the file "saffron.properties", if it exists in classpath
    try (InputStream stream = requireNonNull(classLoader, "classLoader")
        .getResourceAsStream("saffron.properties")) {
      if (stream != null) {
        saffronProperties.load(stream);
      }
    } catch (IOException e) {
      throw new RuntimeException("while reading from saffron.properties file", e);
    } catch (RuntimeException e) {
      if (!"java.security.AccessControlException".equals(e.getClass().getName())) {
        throw e;
      }
    }

    // Merge system and saffron properties, mapping deprecated saffron
    // namespaces to calcite
    final Properties allProperties = new Properties();
    Stream.concat(
        saffronProperties.entrySet().stream(),
        System.getProperties().entrySet().stream())
        .forEach(prop -> {
          String deprecatedKey = (String) prop.getKey();
          String newKey = deprecatedKey
              .replace("net.sf.saffron.", "calcite.")
              .replace("saffron.", "calcite.");
          if (newKey.startsWith("calcite.")) {
            allProperties.setProperty(newKey, (String) prop.getValue());
          }
        });
    return allProperties;
  }

  private final T value;

  private CalciteSystemProperty(String key,
      Function<? super @Nullable String, ? extends T> valueParser) {
    this.value = valueParser.apply(PROPERTIES.getProperty(key));
  }

  /**
   * Returns the value of this property.
   *
   * @return the value of this property or <code>null</code> if a default value has not been
   * defined for this property.
   */
  public T value() {
    return value;
  }
}
