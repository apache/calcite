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
package org.apache.calcite.runtime;

import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonExistsErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A collection of functions used in JSON processing.
 */
public class JsonFunctions {

  private static final Pattern JSON_PATH_BASE =
      Pattern.compile("^\\s*(?<mode>strict|lax)\\s+(?<spec>.+)$",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

  private static final JacksonJsonProvider JSON_PATH_JSON_PROVIDER =
      new JacksonJsonProvider();
  private static final MappingProvider JSON_PATH_MAPPING_PROVIDER =
      new JacksonMappingProvider();
  private static final PrettyPrinter JSON_PRETTY_PRINTER =
      new DefaultPrettyPrinter().withObjectIndenter(
          DefaultIndenter.SYSTEM_LINEFEED_INSTANCE.withLinefeed("\n"));

  private JsonFunctions() {
  }

  private static boolean isScalarObject(Object obj) {
    if (obj instanceof Collection) {
      return false;
    }
    if (obj instanceof Map) {
      return false;
    }
    return true;
  }

  public static String jsonize(Object input) {
    return JSON_PATH_JSON_PROVIDER.toJson(input);
  }

  public static Object dejsonize(String input) {
    return JSON_PATH_JSON_PROVIDER.parse(input);
  }

  public static JsonValueContext jsonValueExpression(String input) {
    try {
      return JsonValueContext.withJavaObj(dejsonize(input));
    } catch (Exception e) {
      return JsonValueContext.withException(e);
    }
  }

  public static JsonPathContext jsonApiCommonSyntax(String input) {
    return jsonApiCommonSyntax(jsonValueExpression(input));
  }

  public static JsonPathContext jsonApiCommonSyntax(JsonValueContext input) {
    return jsonApiCommonSyntax(input, "strict $");
  }

  public static JsonPathContext jsonApiCommonSyntax(String input, String pathSpec) {
    return jsonApiCommonSyntax(jsonValueExpression(input), pathSpec);
  }

  public static JsonPathContext jsonApiCommonSyntax(JsonValueContext input, String pathSpec) {
    try {
      Matcher matcher = JSON_PATH_BASE.matcher(pathSpec);
      if (!matcher.matches()) {
        throw RESOURCE.illegalJsonPathSpec(pathSpec).ex();
      }
      PathMode mode = PathMode.valueOf(matcher.group(1).toUpperCase(Locale.ROOT));
      String pathWff = matcher.group(2);
      DocumentContext ctx;
      switch (mode) {
      case STRICT:
        if (input.hasException()) {
          return JsonPathContext.withStrictException(input.exc);
        }
        ctx = JsonPath.parse(input.obj,
            Configuration
                .builder()
                .jsonProvider(JSON_PATH_JSON_PROVIDER)
                .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                .build());
        break;
      case LAX:
        if (input.hasException()) {
          return JsonPathContext.withJavaObj(PathMode.LAX, null);
        }
        ctx = JsonPath.parse(input.obj,
            Configuration
                .builder()
                .options(Option.SUPPRESS_EXCEPTIONS)
                .jsonProvider(JSON_PATH_JSON_PROVIDER)
                .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                .build());
        break;
      default:
        throw RESOURCE.illegalJsonPathModeInPathSpec(mode.toString(), pathSpec).ex();
      }
      try {
        return JsonPathContext.withJavaObj(mode, ctx.read(pathWff));
      } catch (Exception e) {
        return JsonPathContext.withStrictException(e);
      }
    } catch (Exception e) {
      return JsonPathContext.withUnknownException(e);
    }
  }

  public static Boolean jsonExists(String input, String pathSpec) {
    return jsonExists(jsonApiCommonSyntax(input, pathSpec));
  }

  public static Boolean jsonExists(String input, String pathSpec,
      SqlJsonExistsErrorBehavior errorBehavior) {
    return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior);
  }

  public static Boolean jsonExists(JsonValueContext input, String pathSpec) {
    return jsonExists(jsonApiCommonSyntax(input, pathSpec));
  }

  public static Boolean jsonExists(JsonValueContext input, String pathSpec,
      SqlJsonExistsErrorBehavior errorBehavior) {
    return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior);
  }

  public static Boolean jsonExists(JsonPathContext context) {
    return jsonExists(context, SqlJsonExistsErrorBehavior.FALSE);
  }

  public static Boolean jsonExists(JsonPathContext context,
      SqlJsonExistsErrorBehavior errorBehavior) {
    if (context.hasException()) {
      switch (errorBehavior) {
      case TRUE:
        return Boolean.TRUE;
      case FALSE:
        return Boolean.FALSE;
      case ERROR:
        throw toUnchecked(context.exc);
      case UNKNOWN:
        return null;
      default:
        throw RESOURCE.illegalErrorBehaviorInJsonExistsFunc(
            errorBehavior.toString()).ex();
      }
    } else {
      return context.obj != null;
    }
  }

  public static Object jsonValueAny(String input,
      String pathSpec,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError) {
    return jsonValueAny(
        jsonApiCommonSyntax(input, pathSpec),
        emptyBehavior,
        defaultValueOnEmpty,
        errorBehavior,
        defaultValueOnError);
  }

  public static Object jsonValueAny(JsonValueContext input,
      String pathSpec,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError) {
    return jsonValueAny(
        jsonApiCommonSyntax(input, pathSpec),
        emptyBehavior,
        defaultValueOnEmpty,
        errorBehavior,
        defaultValueOnError);
  }

  public static Object jsonValueAny(JsonPathContext context,
      SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
      Object defaultValueOnEmpty,
      SqlJsonValueEmptyOrErrorBehavior errorBehavior,
      Object defaultValueOnError) {
    final Exception exc;
    if (context.hasException()) {
      exc = context.exc;
    } else {
      Object value = context.obj;
      if (value == null || context.mode == PathMode.LAX
          && !isScalarObject(value)) {
        switch (emptyBehavior) {
        case ERROR:
          throw RESOURCE.emptyResultOfJsonValueFuncNotAllowed().ex();
        case NULL:
          return null;
        case DEFAULT:
          return defaultValueOnEmpty;
        default:
          throw RESOURCE.illegalEmptyBehaviorInJsonValueFunc(
              emptyBehavior.toString()).ex();
        }
      } else if (context.mode == PathMode.STRICT
          && !isScalarObject(value)) {
        exc = RESOURCE.scalarValueRequiredInStrictModeOfJsonValueFunc(
            value.toString()).ex();
      } else {
        return value;
      }
    }
    switch (errorBehavior) {
    case ERROR:
      throw toUnchecked(exc);
    case NULL:
      return null;
    case DEFAULT:
      return defaultValueOnError;
    default:
      throw RESOURCE.illegalErrorBehaviorInJsonValueFunc(
          errorBehavior.toString()).ex();
    }
  }

  public static String jsonQuery(String input,
      String pathSpec,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior) {
    return jsonQuery(
        jsonApiCommonSyntax(input, pathSpec),
        wrapperBehavior, emptyBehavior, errorBehavior);
  }

  public static String jsonQuery(JsonValueContext input,
      String pathSpec,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior) {
    return jsonQuery(
        jsonApiCommonSyntax(input, pathSpec),
        wrapperBehavior, emptyBehavior, errorBehavior);
  }

  public static String jsonQuery(JsonPathContext context,
      SqlJsonQueryWrapperBehavior wrapperBehavior,
      SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
      SqlJsonQueryEmptyOrErrorBehavior errorBehavior) {
    final Exception exc;
    if (context.hasException()) {
      exc = context.exc;
    } else {
      Object value;
      if (context.obj == null) {
        value = null;
      } else {
        switch (wrapperBehavior) {
        case WITHOUT_ARRAY:
          value = context.obj;
          break;
        case WITH_UNCONDITIONAL_ARRAY:
          value = Collections.singletonList(context.obj);
          break;
        case WITH_CONDITIONAL_ARRAY:
          if (context.obj instanceof Collection) {
            value = context.obj;
          } else {
            value = Collections.singletonList(context.obj);
          }
          break;
        default:
          throw RESOURCE.illegalWrapperBehaviorInJsonQueryFunc(
              wrapperBehavior.toString()).ex();
        }
      }
      if (value == null || context.mode == PathMode.LAX
          && isScalarObject(value)) {
        switch (emptyBehavior) {
        case ERROR:
          throw RESOURCE.emptyResultOfJsonQueryFuncNotAllowed().ex();
        case NULL:
          return null;
        case EMPTY_ARRAY:
          return "[]";
        case EMPTY_OBJECT:
          return "{}";
        default:
          throw RESOURCE.illegalEmptyBehaviorInJsonQueryFunc(
              emptyBehavior.toString()).ex();
        }
      } else if (context.mode == PathMode.STRICT && isScalarObject(value)) {
        exc = RESOURCE.arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(
            value.toString()).ex();
      } else {
        try {
          return jsonize(value);
        } catch (Exception e) {
          exc = e;
        }
      }
    }
    switch (errorBehavior) {
    case ERROR:
      throw toUnchecked(exc);
    case NULL:
      return null;
    case EMPTY_ARRAY:
      return "[]";
    case EMPTY_OBJECT:
      return "{}";
    default:
      throw RESOURCE.illegalErrorBehaviorInJsonQueryFunc(
          errorBehavior.toString()).ex();
    }
  }

  public static String jsonObject(SqlJsonConstructorNullClause nullClause,
      Object... kvs) {
    assert kvs.length % 2 == 0;
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      String k = (String) kvs[i];
      Object v = kvs[i + 1];
      if (k == null) {
        throw RESOURCE.nullKeyOfJsonObjectNotAllowed().ex();
      }
      if (v == null) {
        if (nullClause == SqlJsonConstructorNullClause.NULL_ON_NULL) {
          map.put(k, null);
        }
      } else {
        map.put(k, v);
      }
    }
    return jsonize(map);
  }

  public static void jsonObjectAggAdd(Map map, String k, Object v,
      SqlJsonConstructorNullClause nullClause) {
    if (k == null) {
      throw RESOURCE.nullKeyOfJsonObjectNotAllowed().ex();
    }
    if (v == null) {
      if (nullClause == SqlJsonConstructorNullClause.NULL_ON_NULL) {
        map.put(k, null);
      }
    } else {
      map.put(k, v);
    }
  }

  public static String jsonArray(SqlJsonConstructorNullClause nullClause,
      Object... elements) {
    List<Object> list = new ArrayList<>();
    for (Object element : elements) {
      if (element == null) {
        if (nullClause == SqlJsonConstructorNullClause.NULL_ON_NULL) {
          list.add(null);
        }
      } else {
        list.add(element);
      }
    }
    return jsonize(list);
  }

  public static void jsonArrayAggAdd(List list, Object element,
      SqlJsonConstructorNullClause nullClause) {
    if (element == null) {
      if (nullClause == SqlJsonConstructorNullClause.NULL_ON_NULL) {
        list.add(null);
      }
    } else {
      list.add(element);
    }
  }

  public static String jsonPretty(String input) {
    return jsonPretty(jsonValueExpression(input));
  }

  public static String jsonPretty(JsonValueContext input) {
    try {
      return JSON_PATH_JSON_PROVIDER.getObjectMapper().writer(JSON_PRETTY_PRINTER)
          .writeValueAsString(input.obj);
    } catch (Exception e) {
      throw RESOURCE.exceptionWhileSerializingToJson(Objects.toString(input.obj)).ex();
    }
  }

  public static String jsonType(String input) {
    return jsonType(jsonValueExpression(input));
  }

  public static String jsonType(JsonValueContext input) {
    final String result;
    final Object val = input.obj;
    try {
      if (val instanceof Integer) {
        result = "INTEGER";
      } else if (val instanceof String) {
        result = "STRING";
      } else if (val instanceof Float) {
        result = "FLOAT";
      } else if (val instanceof Double) {
        result = "DOUBLE";
      } else if (val instanceof Long) {
        result = "LONG";
      } else if (val instanceof Boolean) {
        result = "BOOLEAN";
      } else if (val instanceof Date) {
        result = "DATE";
      } else if (val instanceof Map) {
        result = "OBJECT";
      } else if (val instanceof Collection) {
        result = "ARRAY";
      } else if (val == null) {
        result = "NULL";
      } else {
        throw RESOURCE.invalidInputForJsonType(val.toString()).ex();
      }
      return result;
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonType(val.toString()).ex();
    }
  }

  public static Integer jsonDepth(String input) {
    return jsonDepth(jsonValueExpression(input));
  }

  public static Integer jsonDepth(JsonValueContext input) {
    final Integer result;
    final Object o = input.obj;
    try {
      if (o == null) {
        result = null;
      } else {
        result = calculateDepth(o);
      }
      return result;
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonDepth(o.toString()).ex();
    }
  }

  private static Integer calculateDepth(Object o) {
    if (isScalarObject(o)) {
      return 1;
    }
    Queue<Object> q = new LinkedList<>();
    int depth = 0;
    q.add(o);

    while (!q.isEmpty()) {
      int size = q.size();
      for (int i = 0; i < size; ++i) {
        Object obj = q.poll();
        if (obj instanceof Map) {
          for (Object value : ((LinkedHashMap) obj).values()) {
            q.add(value);
          }
        } else if (obj instanceof Collection) {
          for (Object value : (Collection) obj) {
            q.add(value);
          }
        }
      }
      ++depth;
    }
    return depth;
  }

  public static Integer jsonLength(String input) {
    return jsonLength(jsonApiCommonSyntax(input));
  }

  public static Integer jsonLength(JsonValueContext input) {
    return jsonLength(jsonApiCommonSyntax(input));
  }

  public static Integer jsonLength(String input, String pathSpec) {
    return jsonLength(jsonApiCommonSyntax(input, pathSpec));
  }

  public static Integer jsonLength(JsonValueContext input, String pathSpec) {
    return jsonLength(jsonApiCommonSyntax(input, pathSpec));
  }

  public static Integer jsonLength(JsonPathContext context) {
    final Integer result;
    final Object value;
    try {
      if (context.hasException()) {
        throw toUnchecked(context.exc);
      }
      value = context.obj;

      if (value == null) {
        result = null;
      } else {
        if (value instanceof Collection) {
          result = ((Collection) value).size();
        } else if (value instanceof Map) {
          result = ((LinkedHashMap) value).size();
        } else if (isScalarObject(value)) {
          result = 1;
        } else {
          result = 0;
        }
      }
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonLength(
          context.toString()).ex();
    }
    return result;
  }

  public static String jsonKeys(String input) {
    return jsonKeys(jsonApiCommonSyntax(input));
  }

  public static String jsonKeys(JsonValueContext input) {
    return jsonKeys(jsonApiCommonSyntax(input));
  }

  public static String jsonKeys(String input, String pathSpec) {
    return jsonKeys(jsonApiCommonSyntax(input, pathSpec));
  }

  public static String jsonKeys(JsonValueContext input, String pathSpec) {
    return jsonKeys(jsonApiCommonSyntax(input, pathSpec));
  }

  public static String jsonKeys(JsonPathContext context) {
    List<String> list = new ArrayList<>();
    final Object value;
    try {
      if (context.hasException()) {
        throw toUnchecked(context.exc);
      }
      value = context.obj;

      if ((value == null) || (value instanceof Collection)
          || isScalarObject(value)) {
        list = null;
      } else if (value instanceof Map) {
        for (Object key : ((LinkedHashMap) value).keySet()) {
          list.add(key.toString());
        }
      }
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonKeys(
          context.toString()).ex();
    }
    return jsonize(list);
  }

  public static String jsonRemove(String input, String... pathSpecs) {
    return jsonRemove(jsonValueExpression(input), pathSpecs);
  }

  public static String jsonRemove(JsonValueContext input, String... pathSpecs) {
    try {
      DocumentContext ctx = JsonPath.parse(input.obj,
          Configuration
              .builder()
              .options(Option.SUPPRESS_EXCEPTIONS)
              .jsonProvider(JSON_PATH_JSON_PROVIDER)
              .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
              .build());
      for (String pathSpec : pathSpecs) {
        if ((pathSpec != null) && (ctx.read(pathSpec) != null)) {
          ctx.delete(pathSpec);
        }
      }
      return ctx.jsonString();
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonRemove(
          input.toString(), Arrays.toString(pathSpecs)).ex();
    }
  }

  public static Integer jsonStorageSize(String input) {
    return jsonStorageSize(jsonValueExpression(input));
  }

  public static Integer jsonStorageSize(JsonValueContext input) {
    try {
      return JSON_PATH_JSON_PROVIDER.getObjectMapper()
          .writeValueAsBytes(input.obj).length;
    } catch (Exception e) {
      throw RESOURCE.invalidInputForJsonStorageSize(Objects.toString(input.obj)).ex();
    }
  }

  public static boolean isJsonValue(String input) {
    try {
      dejsonize(input);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isJsonObject(String input) {
    try {
      Object o = dejsonize(input);
      return o instanceof Map;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isJsonArray(String input) {
    try {
      Object o = dejsonize(input);
      return o instanceof Collection;
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isJsonScalar(String input) {
    try {
      Object o = dejsonize(input);
      return !(o instanceof Map) && !(o instanceof Collection);
    } catch (Exception e) {
      return false;
    }
  }

  private static RuntimeException toUnchecked(Exception e) {
    return Util.toUnchecked(e);
  }

  /**
   * Returned path context of JsonApiCommonSyntax, public for testing.
   */
  public static class JsonPathContext {
    public final PathMode mode;
    public final Object obj;
    public final Exception exc;

    private JsonPathContext(Object obj, Exception exc) {
      this(PathMode.NONE, obj, exc);
    }

    private JsonPathContext(PathMode mode, Object obj, Exception exc) {
      assert obj == null || exc == null;
      this.mode = mode;
      this.obj = obj;
      this.exc = exc;
    }

    public boolean hasException() {
      return exc != null;
    }

    public static JsonPathContext withUnknownException(Exception exc) {
      return new JsonPathContext(PathMode.UNKNOWN, null, exc);
    }

    public static JsonPathContext withStrictException(Exception exc) {
      return new JsonPathContext(PathMode.STRICT, null, exc);
    }

    public static JsonPathContext withJavaObj(PathMode mode, Object obj) {
      if (mode == PathMode.UNKNOWN) {
        throw RESOURCE.illegalJsonPathMode(mode.toString()).ex();
      }
      if (mode == PathMode.STRICT && obj == null) {
        throw RESOURCE.strictPathModeRequiresNonEmptyValue().ex();
      }
      return new JsonPathContext(mode, obj, null);
    }

    @Override public String toString() {
      return "JsonPathContext{"
          + "mode=" + mode
          + ", obj=" + obj
          + ", exc=" + exc
          + '}';
    }
  }

  /**
   * The Java output of {@link org.apache.calcite.sql.fun.SqlJsonValueExpressionOperator}.
   */
  public static class JsonValueContext {
    @JsonValue
    public final Object obj;
    public final Exception exc;

    private JsonValueContext(Object obj, Exception exc) {
      assert obj == null || exc == null;
      this.obj = obj;
      this.exc = exc;
    }

    public static JsonValueContext withJavaObj(Object obj) {
      return new JsonValueContext(obj, null);
    }

    public static JsonValueContext withException(Exception exc) {
      return new JsonValueContext(null, exc);
    }

    public boolean hasException() {
      return exc != null;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JsonValueContext jsonValueContext = (JsonValueContext) o;
      return Objects.equals(obj, jsonValueContext.obj);
    }

    @Override public int hashCode() {
      return Objects.hash(obj);
    }

    @Override public String toString() {
      return Objects.toString(obj);
    }
  }

  /**
   * Path spec has two different modes: lax mode and strict mode.
   * Lax mode suppresses any thrown exception and returns null,
   * whereas strict mode throws exceptions.
   */
  public enum PathMode {
    LAX,
    STRICT,
    UNKNOWN,
    NONE
  }
}

// End JsonFunctions.java
