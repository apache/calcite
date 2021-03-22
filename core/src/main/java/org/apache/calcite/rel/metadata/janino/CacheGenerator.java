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
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;

import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.argList;
import static org.apache.calcite.rel.metadata.janino.CodeGeneratorUtil.paramList;

/**
 * Generates caching code for janino backed metadata.
 */
class CacheGenerator {

  private CacheGenerator() {
  }

  static void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
    buff.append("  private final Object ");
    appendKeyName(buff, methodIndex);
    buff.append(" = new ")
        .append(DescriptiveCacheKey.class.getName())
        .append("(\"")
        .append(method.toString())
        .append("\");\n");
  }

  public static void cachedMethod(StringBuilder buff, Method method, int methodIndex) {
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
    paramList(buff, method, 2)
        .append(") {\n");
    buff.append("    final java.util.List key = ")
        .append(
            (method.getParameterTypes().length < 6
                ? org.apache.calcite.runtime.FlatLists.class
                : ImmutableList.class).getName())
        .append(".of(");
    appendKeyName(buff, methodIndex);
    safeArgList(buff, method)
        .append(");\n")
        .append("    final Object v = mq.map.get(r, key);\n")
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
    argList(buff, method, 2)
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


  /** Returns e.g. ", ignoreNulls". */
  private static StringBuilder safeArgList(StringBuilder buff, Method method) {
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

  private static void appendKeyName(StringBuilder buff, int methodIndex) {
    buff.append("methodKey").append(methodIndex);
  }
}
