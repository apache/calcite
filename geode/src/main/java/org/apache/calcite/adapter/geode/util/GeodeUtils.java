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
package org.apache.calcite.adapter.geode.util;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Util;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Utilities for the Geode adapter.
 */
public class GeodeUtils {

  protected static final Logger LOGGER = LoggerFactory.getLogger(GeodeUtils.class.getName());

  /**
   * Cache for the client proxy regions created in the current ClientCache.
   */
  private static final Map<String, Region> REGION_MAP = new ConcurrentHashMap<>();

  private static String currentLocatorHost = "";
  private static int currentLocatorPort = -1;

  private static final JavaTypeFactoryExtImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryExtImpl();

  private GeodeUtils() {
  }

  /** Checks whether the {@code pdxSerializablePackagePath} schema operand is enabled.
   *
   * <p>The operand configures Geode's {@link ReflectionBasedAutoSerializer}. It is disabled by
   * default; deployments that need it can opt back in by setting the system property
   * {@code calcite.geode.pdxSerializablePackagePath.allowed} to {@code "true"}.
   *
   * @param pdxSerializablePackagePath operand value, or null if not set
   * @see CalciteSystemProperty#GEODE_PDX_PACKAGE_PATH_ALLOWED
   */
  public static void checkPdxPackagePathOperand(
      @Nullable String pdxSerializablePackagePath) {
    if (pdxSerializablePackagePath == null
        || CalciteSystemProperty.GEODE_PDX_PACKAGE_PATH_ALLOWED.value()) {
      return;
    }
    throw new IllegalStateException("Operand 'pdxSerializablePackagePath'"
        + " is disabled by default; set system property"
        + " 'calcite.geode.pdxSerializablePackagePath.allowed' to 'true' to enable it");
  }

  /**
   * Creates a Geode client instance connected to locator and configured to
   * support PDX instances.
   *
   * <p>If an old instance exists, it will be destroyed and re-created.
   *
   * @param locatorHost               Locator's host address
   * @param locatorPort               Locator's port
   * @param autoSerializerPackagePath package name of the Domain classes loaded in the regions
   * @return Returns a Geode {@link ClientCache} instance connected to Geode cluster
   */
  public static synchronized ClientCache createClientCache(String locatorHost,
      int locatorPort, String autoSerializerPackagePath,
      boolean readSerialized) {
    if (locatorPort != currentLocatorPort
        || !equalsIgnoreCase(currentLocatorHost, locatorHost)) {
      LOGGER.info("Close existing ClientCache ["
          + currentLocatorHost + ":" + currentLocatorPort + "] for new Locator connection at: ["
          + locatorHost + ":" + locatorPort + "]");
      currentLocatorHost = locatorHost;
      currentLocatorPort = locatorPort;
      closeClientCache();
    }

    try {
      // If exists returns the existing client cache. This requires that the pre-created
      // client proxy regions can also be resolved from the regionMap
      return ClientCacheFactory.getAnyInstance();
    } catch (CacheClosedException cce) {
      // Do nothing if there is no existing instance
    }

    return new ClientCacheFactory()
        .addPoolLocator(locatorHost, locatorPort)
        .setPdxSerializer(new ReflectionBasedAutoSerializer(autoSerializerPackagePath))
        .setPdxReadSerialized(readSerialized)
        .create();
  }

  public static synchronized void closeClientCache() {
    try {
      ClientCacheFactory.getAnyInstance().close();
    } catch (CacheClosedException cce) {
      // Do nothing if there is no existing instance
    }
    REGION_MAP.clear();
  }

  private static boolean equalsIgnoreCase(@Nullable String a, @Nullable String b) {
    return a == null ? b == null : b != null && a.equalsIgnoreCase(b);
  }

  /**
   * Obtains a proxy pointing to an existing Region on the server.
   *
   * @param cache {@link GemFireCache} instance to interact with the Geode server
   * @param regionName  Name of the region to create proxy for.
   * @return Returns a Region proxy to a remote (on the Server) regions.
   */
  public static synchronized Region createRegion(GemFireCache cache, String regionName) {
    requireNonNull(cache, "cache");
    requireNonNull(regionName, "regionName");
    Region region = REGION_MAP.get(regionName);
    if (region == null) {
      try {
        region = ((ClientCache) cache)
            .createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(regionName);
      } catch (IllegalStateException | RegionExistsException e) {
        // means this is a server cache (probably part of embedded testing
        // or clientCache is passed directly)
        region = cache.getRegion(regionName);
      }

      REGION_MAP.put(regionName, region);
    }

    return region;
  }

  /**
   * Converts a Geode object into a Row tuple.
   *
   * @param relDataTypeFields Table relation types
   * @param geodeResultObject Object value returned by Geode query
   * @return List of objects values corresponding to the relDataTypeFields
   */
  public static @Nullable Object convertToRowValues(
      List<RelDataTypeField> relDataTypeFields, Object geodeResultObject) {

    Object values;

    if (geodeResultObject instanceof Struct) {
      values = handleStructEntry(relDataTypeFields, geodeResultObject);
    } else if (geodeResultObject instanceof PdxInstance) {
      values = handlePdxInstanceEntry(relDataTypeFields, geodeResultObject);
    } else {
      values = handleJavaObjectEntry(relDataTypeFields, geodeResultObject);
    }

    return values;
  }

  private static Object handleStructEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    Struct struct = (Struct) obj;

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      Type javaType = JAVA_TYPE_FACTORY.getJavaClass(relDataTypeField.getType());
      Object rawValue;
      try {
        rawValue = struct.get(relDataTypeField.getName());
      } catch (IllegalArgumentException e) {
        rawValue = "<error>";
        System.err.println("Could find field : " + relDataTypeField.getName());
        e.printStackTrace();
      }
      values[index++] = convert(rawValue, (Class) javaType);
    }

    if (values.length == 1) {
      return values[0];
    }

    return values;
  }

  private static Object handlePdxInstanceEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    PdxInstance pdxEntry = (PdxInstance) obj;

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      Type javaType = JAVA_TYPE_FACTORY.getJavaClass(relDataTypeField.getType());
      Object rawValue = pdxEntry.getField(relDataTypeField.getName());
      values[index++] = convert(rawValue, (Class) javaType);
    }

    if (values.length == 1) {
      return values[0];
    }

    return values;
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  private static @Nullable Object handleJavaObjectEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    Class<?> clazz = obj.getClass();
    if (relDataTypeFields.size() == 1) {
      try {
        Field javaField = clazz.getDeclaredField(relDataTypeFields.get(0).getName());
        javaField.setAccessible(true);
        return javaField.get(obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      try {
        Field javaField = clazz.getDeclaredField(relDataTypeField.getName());
        javaField.setAccessible(true);
        values[index++] = javaField.get(obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return values;
  }

  @SuppressWarnings("JavaUtilDate")
  private static Object convert(Object o, Class clazz) {
    if (o == null) {
      return null;
    }
    Primitive primitive = Primitive.of(clazz);
    if (primitive != null) {
      clazz = primitive.boxClass;
    } else {
      primitive = Primitive.ofBox(clazz);
    }
    if (clazz == null) {
      return o.toString();
    }
    if (Map.class.isAssignableFrom(clazz)
            && o instanceof PdxInstance) {
      // This is in case of nested Objects!
      return Util.toString(
              ((PdxInstance) o).getFieldNames(), "PDX[", ",", "]");
    }
    if (clazz.isInstance(o)) {
      return o;
    }
    if (o instanceof Date && primitive != null) {
      o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
    }
    if (o instanceof Number && primitive != null) {
      return primitive.number((Number) o);
    }
    return o;
  }

  /**
   * Extract the first entity of each Regions and use it to build a table types.
   *
   * @param region existing region
   * @return derived data type.
   */
  public static RelDataType autodetectRelTypeFromRegion(Region<?, ?> region) {
    requireNonNull(region, "region");

    // try to detect type using value constraints (if they exists)
    final Class<?> constraint = region.getAttributes().getValueConstraint();
    if (constraint != null && !PdxInstance.class.isAssignableFrom(constraint)) {
      return new JavaTypeFactoryExtImpl().createStructType(constraint);
    }

    final Iterator<?> iter;
    if (region.getAttributes().getPoolName() == null) {
      // means current cache is server (not ClientCache)
      iter = region.keySet().iterator();
    } else {
      // for ClientCache
      iter = region.keySetOnServer().iterator();
    }

    if (!iter.hasNext()) {
      String message = String.format(Locale.ROOT, "Region %s is empty, can't "
          + "autodetect type(s)", region.getName());
      throw new IllegalStateException(message);
    }

    final Object entry = region.get(iter.next());
    return createRelDataType(entry);
  }

  /**
   * OQL has no way to escape identifiers, so an identifier that is copied
   * verbatim into the generated query must be a simple alphanumeric name (with
   * optional dotted path or bracketed integer subscript for nested access).
   * Anything else could shift the boundaries of the emitted statement.
   */
  private static final Pattern SAFE_OQL_IDENTIFIER =
      Pattern.compile(
          "[A-Za-z_$][A-Za-z0-9_$]*+(?:\\.[A-Za-z_$][A-Za-z0-9_$]*+|\\[[0-9]++\\])*+");

  /**
   * Returns whether {@code name} may be emitted verbatim as an OQL identifier
   * (a field name, alias, or dotted path). Since OQL provides no way to quote
   * an identifier, callers should refuse to push down when this returns false.
   */
  public static boolean isSafeOqlIdentifier(String name) {
    return name != null && SAFE_OQL_IDENTIFIER.matcher(name).matches();
  }

  /**
   * Validates that {@code name} may be emitted verbatim into a generated OQL
   * statement, throwing a {@link RuntimeException} otherwise.
   */
  public static String checkSafeOqlIdentifier(String name) {
    if (!isSafeOqlIdentifier(name)) {
      throw new RuntimeException(
          "Cannot push down expression: not a simple OQL identifier: " + name);
    }
    return name;
  }

  // Create Relational Type by inferring a Geode entry or response instance.
  private static RelDataType createRelDataType(Object regionEntry) {
    JavaTypeFactoryExtImpl typeFactory = new JavaTypeFactoryExtImpl();
    if (regionEntry instanceof PdxInstance) {
      return typeFactory.createPdxType((PdxInstance) regionEntry);
    } else {
      return typeFactory.createStructType(regionEntry.getClass());
    }
  }

}
