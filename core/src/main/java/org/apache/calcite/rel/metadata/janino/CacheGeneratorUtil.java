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
package org.apache.calcite.rel.metadata.janino;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.argList;
import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.paramList;

/**
 * Generates caching code for janino backed metadata.
 */
class CacheGeneratorUtil {

  private CacheGeneratorUtil() {
  }

  static void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
    selectStrategy(method).cacheProperties(buff, method, methodIndex);
  }

  static void cachedMethod(StringBuilder buff, Method method, int methodIndex) {
    String delRelClass = DelegatingMetadataRel.class.getName();
    buff.append("  public ")
        .append(method.getReturnType().getName())
        .append(" ")
        .append(method.getName())
        .append("(\n")
        .append("      ")
        .append(RelNode.class.getName())
        .append(" r,\n")
        .append("      ")
        .append(RelMetadataQuery.class.getName())
        .append(" mq");
    paramList(buff, method)
        .append(") {\n")
        .append("    while (r instanceof ").append(delRelClass).append(") {\n")
        .append("      r = ((").append(delRelClass).append(") r).getMetadataDelegateRel();\n")
        .append("    }\n")
        .append("    final Object key;\n");
    selectStrategy(method).cacheKeyBlock(buff, method, methodIndex);
    buff.append("    final Object v = mq.map.get(r, key);\n")
        .append("    if (v != null) {\n")
        .append("      if (v == ")
        .append(NullSentinel.class.getName())
        .append(".ACTIVE) {\n")
        .append("        throw new ")
        .append(CyclicMetadataException.class.getName())
        .append("();\n")
        .append("      }\n")
        .append("      if (v == ")
        .append(NullSentinel.class.getName())
        .append(".INSTANCE) {\n")
        .append("        return null;\n")
        .append("      }\n")
        .append("      return (")
        .append(method.getReturnType().getName())
        .append(") v;\n")
        .append("    }\n")
        .append("    mq.map.put(r, key,")
        .append(NullSentinel.class.getName())
        .append(".ACTIVE);\n")
        .append("    try {\n")
        .append("      final ")
        .append(method.getReturnType().getName())
        .append(" x = ")
        .append(method.getName())
        .append("_(r, mq");
    argList(buff, method)
        .append(");\n")
        .append("      mq.map.put(r, key, ")
        .append(NullSentinel.class.getName())
        .append(".mask(x));\n")
        .append("      return x;\n")
        .append("    } catch (")
        .append(Exception.class.getName())
        .append(" e) {\n")
        .append("      mq.map.row(r).clear();\n")
        .append("      throw e;\n")
        .append("    }\n")
        .append("  }\n")
        .append("\n");
  }

  private static void appendKeyName(StringBuilder buff, int methodIndex) {
    buff.append("methodKey").append(methodIndex);
  }

  private static void appendKeyName(StringBuilder buff, int methodIndex, String arg) {
    buff.append("methodKey")
        .append(methodIndex).append(arg);
  }

  private static CacheKeyStrategy selectStrategy(Method method) {
    switch (method.getParameterCount()) {
    case 2:
      return CacheKeyStrategy.NO_ARG;
    case 3:
      Class<?> clazz = method.getParameterTypes()[2];
      if (clazz.equals(boolean.class)) {
        return CacheKeyStrategy.BOOLEAN_ARG;
      } else if (Enum.class.isAssignableFrom(clazz)) {
        return CacheKeyStrategy.ENUM_ARG;
      } else if (clazz.equals(int.class)) {
        return CacheKeyStrategy.INT_ARG;
      } else {
        return CacheKeyStrategy.DEFAULT;
      }
    default:
      return CacheKeyStrategy.DEFAULT;
    }
  }

  private static StringBuilder newDescriptiveCacheKey(StringBuilder buff,
      Method method, String arg) {
    return buff.append("      new ")
        .append(DescriptiveCacheKey.class.getName())
        .append("(\"")
        .append(method.getReturnType().getSimpleName())
        .append(" ")
        .append(method.getDeclaringClass().getSimpleName())
        .append(".")
        .append(method.getName())
        .append("(")
        .append(arg)
        .append(")")
        .append("\");\n");
  }

  /**
   * Generates a set of properties that are to be used by a fragment of code to
   * efficiently create metadata keys.
   */
  private enum CacheKeyStrategy {
    /**
     * Generates an immutable method key, then during each call instantiates a new list to all
     * the arguments.
     *
     * Example:
     * <code>
     *     private final Object method_key_0 =
     *        new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
     *
     *  ...
     *
     *   public java.lang.Double getDistinctRowCount(
     *       org.apache.calcite.rel.RelNode r,
     *       org.apache.calcite.rel.metadata.RelMetadataQuery mq,
     *       org.apache.calcite.util.ImmutableBitSet a2,
     *       org.apache.calcite.rex.RexNode a3) {
     *     final Object key;
     *     key = org.apache.calcite.runtime.FlatLists.of(method_key_0, org.apache.calcite.rel
     * .metadata.NullSentinel.mask(a2), a3);
     *     final Object v = mq.map.get(r, key);
     *     if (v != null) {
     *      ...
     * </code>
     */
    DEFAULT {
      @Override void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        String args = ImmutableList.copyOf(method.getParameterTypes()).stream()
            .map(Class::getSimpleName)
            .collect(Collectors.joining(", "));
        buff.append("  private final Object ");
        appendKeyName(buff, methodIndex);
        buff.append(" =\n");
        newDescriptiveCacheKey(buff, method, args);
      }

      @Override void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex) {
        buff.append("    key = ")
            .append(
                (method.getParameterCount() < 6
                    ? org.apache.calcite.runtime.FlatLists.class
                    : ImmutableList.class).getName())
            .append(".of(");
        appendKeyName(buff, methodIndex);
        safeArgList(buff, method)
            .append(");\n");
      }

      /** Returns e.g. ", ignoreNulls". */
      private StringBuilder safeArgList(StringBuilder buff, Method method) {
        //We ignore the first 2 arguments since they are included other ways.
        for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())
            .subList(2, method.getParameterCount())) {
          if (Primitive.is(t.e) || RexNode.class.isAssignableFrom(t.e)) {
            buff.append(", a").append(t.i);
          } else {
            buff.append(", ").append(NullSentinel.class.getName())
                .append(".mask(a").append(t.i).append(")");
          }
        }
        return buff;
      }
    },
    /**
     * Generates an immutable key that is reused across all calls.
     *
     * Example:
     * <code>
     *     private final Object method_key_0 =
     *       new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
     *
     *   ...
     *
     *   public java.lang.Double getPercentageOriginalRows(
     *       org.apache.calcite.rel.RelNode r,
     *       org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
     *     final Object key;
     *     key = method_key_0;
     *     final Object v = mq.map.get(r, key);
     * </code>
     */
    NO_ARG {
      @Override void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        buff.append("  private final Object ");
        appendKeyName(buff, methodIndex);
        buff.append(" =\n");
        newDescriptiveCacheKey(buff, method, "");
      }

      @Override void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex) {
        buff.append("    key = ");
        appendKeyName(buff, methodIndex);
        buff.append(";\n");
      }
    },
    /**
     * Generates immutable cache keys for metadata calls with single enum argument.
     *
     * Example:
     * <code>
     *   private final Object method_key_0Null =
     *       new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey(
     *         "Boolean isVisibleInExplain(null)");
     *   private final Object[] method_key_0 =
     *       org.apache.calcite.rel.metadata.janino.CacheUtil.generateEnum(
     *         "Boolean isVisibleInExplain", org.apache.calcite.sql.SqlExplainLevel.values());
     *
     *   ...
     *
     *   public java.lang.Boolean isVisibleInExplain(
     *       org.apache.calcite.rel.RelNode r,
     *       org.apache.calcite.rel.metadata.RelMetadataQuery mq,
     *       org.apache.calcite.sql.SqlExplainLevel a2) {
     *     final Object key;
     *     if (a2 == null) {
     *       key = method_key_0Null;
     *     } else {
     *       key = method_key_0[a2.ordinal()];
     *     }
     * </code>
     */
    ENUM_ARG {
      @Override void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex) {
        buff.append("    if (a2 == null) {\n")
            .append("      key = ");
        appendKeyName(buff, methodIndex, "Null");
        buff.append(";\n")
            .append("    } else {\n")
            .append("      key = ");
        appendKeyName(buff, methodIndex);
        buff.append("[a2.ordinal()];\n")
            .append("    }\n");
      }

      @Override void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        assert method.getParameterCount() == 3;
        Class<?> clazz = method.getParameterTypes()[2];
        assert Enum.class.isAssignableFrom(clazz);
        buff.append("  private final Object ");
        appendKeyName(buff, methodIndex, "Null");
        buff.append(" =\n");
        newDescriptiveCacheKey(buff, method, "null")
            .append("  private final Object[] ");
        appendKeyName(buff, methodIndex);
        buff.append(" =\n")
            .append("      ")
            .append(CacheUtil.class.getName())
            .append(".generateEnum(\"")
            .append(method.getReturnType().getSimpleName())
            .append(" ")
            .append(method.getName())
            .append("\", ")
            .append(clazz.getName())
            .append(".values());\n");
      }
    },
    /**
     * Generates 2 immutable keys for functions that only take a single boolean arg.
     *
     * Example:
     * <code>
     *  private final Object method_key_0True =
     *       new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
     *   private final Object method_key_0False =
     *       new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
     *
     *   ...
     *
     *   public java.util.Set getUniqueKeys(
     *       org.apache.calcite.rel.RelNode r,
     *       org.apache.calcite.rel.metadata.RelMetadataQuery mq,
     *       boolean a2) {
     *     final Object key;
     *     key = a2 ? method_key_0True : method_key_0False;
     *     final Object v = mq.map.get(r, key);
     *     ...
     * </code>
     */
    BOOLEAN_ARG {
      @Override void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex) {
        buff.append("    key = a2 ? ");
        appendKeyName(buff, methodIndex, "True");
        buff.append(" : ");
        appendKeyName(buff, methodIndex, "False");
        buff.append(";\n");
      }

      @Override void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        assert method.getParameterCount() == 3;
        assert method.getParameterTypes()[2].equals(boolean.class);
        buff.append("  private final Object ");
        appendKeyName(buff, methodIndex, "True");
        buff.append(" =\n");
        newDescriptiveCacheKey(buff, method, "true")
            .append("  private final Object ");
        appendKeyName(buff, methodIndex, "False");
        buff.append(" =\n");
        newDescriptiveCacheKey(buff, method, "false");
      }
    },
    /**
     * Uses a flyweight for fixed range, otherwise instantiates a new list with the arguement in it.
     *
     * Example:
     * <code>
     *   private final Object method_key_0 =
     *         new org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey("...");
     *   private final Object[] method_key_0FlyWeight =
     *       org.apache.calcite.rel.metadata.janino.CacheUtil.generateRange(
     *         "java.util.Set getColumnOrigins", -256, 256);
     *
     *   ...
     *
     *   public java.util.Set getColumnOrigins(
     *       org.apache.calcite.rel.RelNode r,
     *       org.apache.calcite.rel.metadata.RelMetadataQuery mq,
     *       int a2) {
     *     final Object key;
     *     if (a2 &gt;= -256 && a2 &lt; 256) {
     *       key = method_key_0FlyWeight[a2 + 256];
     *     } else {
     *       key = org.apache.calcite.runtime.FlatLists.of(method_key_0, a2);
     *     }
     * </code>
     */
    INT_ARG {
      private final int min = -256;
      private final int max = 256;

      @Override void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex) {
        assert method.getParameterCount() == 3;
        assert method.getParameterTypes()[2] == int.class;
        buff.append("    if (a2 >= ").append(min).append(" && a2 < ").append(max)
            .append(") {\n")
            .append("      key = ");
        appendKeyName(buff, methodIndex, "FlyWeight");
        buff.append("[a2 + ").append(-min).append("];\n")
            .append("    } else {\n")
            .append("      key = ").append(FlatLists.class.getName()).append(".of(");
        appendKeyName(buff, methodIndex);
        buff.append(", a2);\n")
            .append("    }\n");
      }

      @Override void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        DEFAULT.cacheProperties(buff, method, methodIndex);
        buff.append("  private final Object[] ");
        appendKeyName(buff, methodIndex, "FlyWeight");
        buff.append(" =\n")
            .append("      ")
            .append(CacheUtil.class.getName()).append(".generateRange(\"")
            .append(method.getReturnType().getName()).append(" ").append(method.getName())
            .append("\", ").append(min).append(", ").append(max).append(");\n");
      }
    };
    abstract void cacheKeyBlock(StringBuilder buff, Method method, int methodIndex);
    abstract void cacheProperties(StringBuilder buff, Method method, int methodIndex);
  }
}
