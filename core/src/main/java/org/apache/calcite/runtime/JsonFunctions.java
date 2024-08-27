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

import org.apache.calcite.linq4j.function.Deterministic;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

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

import static org.apache.calcite.config.CalciteSystemProperty.FUNCTION_LEVEL_CACHE_MAX_SIZE;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

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

  private static final String JSON_ROOT_PATH = "$";

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

  public static String jsonize(@Nullable Object input) {
    return JSON_PATH_JSON_PROVIDER.toJson(input);
  }

  public static @Nullable Object dejsonize(String input) {
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
    PathMode mode;
    String pathStr;
    try {
      Matcher matcher = JSON_PATH_BASE.matcher(pathSpec);
      if (!matcher.matches()) {
        mode = PathMode.STRICT;
        pathStr = pathSpec;
      } else {
        mode = PathMode.valueOf(castNonNull(matcher.group(1)).toUpperCase(Locale.ROOT));
        pathStr = castNonNull(matcher.group(2));
      }
      DocumentContext ctx;
      switch (mode) {
      case STRICT:
        if (input.hasException()) {
          return JsonPathContext.withStrictException(pathSpec,
              requireNonNull(input.exc));
        }
        ctx =
            JsonPath.parse(input.obj(),
                Configuration.builder()
                    .jsonProvider(JSON_PATH_JSON_PROVIDER)
                    .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                    .build());
        break;
      case LAX:
        if (input.hasException()) {
          return JsonPathContext.withJavaObj(PathMode.LAX, null);
        }
        ctx =
            JsonPath.parse(input.obj(),
                Configuration.builder()
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .jsonProvider(JSON_PATH_JSON_PROVIDER)
                    .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                    .build());
        break;
      default:
        throw RESOURCE.illegalJsonPathModeInPathSpec(mode.toString(), pathSpec).ex();
      }
      try {
        return JsonPathContext.withJavaObj(mode, ctx.read(pathStr));
      } catch (Exception e) {
        return JsonPathContext.withStrictException(pathSpec, e);
      }
    } catch (Exception e) {
      return JsonPathContext.withUnknownException(e);
    }
  }


  /** State for {@code JSON_EXISTS}, {@code JSON_VALUE}, {@code JSON_QUERY}.
   *
   * <p>Marked deterministic so that the code generator instantiates one once
   * per query, not once per row. */
  @Deterministic
  public static class StatefulFunction {
    private final LoadingCache<String, JsonValueContext> cache =
        CacheBuilder.newBuilder()
            .maximumSize(FUNCTION_LEVEL_CACHE_MAX_SIZE.value())
            .build(CacheLoader.from(JsonFunctions::jsonValueExpression));

    public JsonPathContext jsonApiCommonSyntaxWithCache(String input,
        String pathSpec) {
      return jsonApiCommonSyntax(cache.getUnchecked(input), pathSpec);
    }

    public @Nullable Boolean jsonExists(String input, String pathSpec) {
      return jsonExists(jsonApiCommonSyntaxWithCache(input, pathSpec));
    }

    public @Nullable Boolean jsonExists(String input, String pathSpec,
        SqlJsonExistsErrorBehavior errorBehavior) {
      return jsonExists(jsonApiCommonSyntaxWithCache(input, pathSpec),
          errorBehavior);
    }

    public @Nullable Boolean jsonExists(JsonValueContext input,
        String pathSpec) {
      return jsonExists(jsonApiCommonSyntax(input, pathSpec));
    }

    public @Nullable Boolean jsonExists(JsonValueContext input, String pathSpec,
        SqlJsonExistsErrorBehavior errorBehavior) {
      return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior);
    }

    public @Nullable Boolean jsonExists(JsonPathContext context) {
      return jsonExists(context, SqlJsonExistsErrorBehavior.FALSE);
    }

    public @Nullable Boolean jsonExists(JsonPathContext context,
        SqlJsonExistsErrorBehavior errorBehavior) {
      if (context.hasException()) {
        switch (errorBehavior) {
        case TRUE:
          return Boolean.TRUE;
        case FALSE:
          return Boolean.FALSE;
        case ERROR:
          throw toUnchecked(requireNonNull(context.exc));
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

    public @Nullable Object jsonValue(String input,
        String pathSpec,
        SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
        Object defaultValueOnEmpty,
        SqlJsonValueEmptyOrErrorBehavior errorBehavior,
        Object defaultValueOnError) {
      return jsonValue(
          jsonApiCommonSyntaxWithCache(input, pathSpec),
          emptyBehavior,
          defaultValueOnEmpty,
          errorBehavior,
          defaultValueOnError);
    }

    public @Nullable Object jsonValue(JsonValueContext input,
        String pathSpec,
        SqlJsonValueEmptyOrErrorBehavior emptyBehavior,
        Object defaultValueOnEmpty,
        SqlJsonValueEmptyOrErrorBehavior errorBehavior,
        Object defaultValueOnError) {
      return jsonValue(
          jsonApiCommonSyntax(input, pathSpec),
          emptyBehavior,
          defaultValueOnEmpty,
          errorBehavior,
          defaultValueOnError);
    }

    public @Nullable Object jsonValue(JsonPathContext context,
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
          exc =
              RESOURCE.scalarValueRequiredInStrictModeOfJsonValueFunc(
                  value.toString()).ex();
        } else {
          return value;
        }
      }
      switch (errorBehavior) {
      case ERROR:
        throw toUnchecked(requireNonNull(exc, "exc"));
      case NULL:
        return null;
      case DEFAULT:
        return defaultValueOnError;
      default:
        throw RESOURCE.illegalErrorBehaviorInJsonValueFunc(
            errorBehavior.toString()).ex();
      }
    }

    public @Nullable Object jsonQuery(
        String input,
        String pathSpec,
        SqlJsonQueryWrapperBehavior wrapperBehavior,
        SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
        SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
        boolean jsonize) {
      return jsonQuery(
          jsonApiCommonSyntaxWithCache(input, pathSpec),
          wrapperBehavior, emptyBehavior, errorBehavior, jsonize);
    }

    public @Nullable Object jsonQuery(JsonValueContext input,
        String pathSpec,
        SqlJsonQueryWrapperBehavior wrapperBehavior,
        SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
        SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
        boolean jsonize) {
      return jsonQuery(
          jsonApiCommonSyntax(input, pathSpec),
          wrapperBehavior, emptyBehavior, errorBehavior, jsonize);
    }

    public @Nullable Object jsonQuery(
        JsonPathContext context,
        SqlJsonQueryWrapperBehavior wrapperBehavior,
        SqlJsonQueryEmptyOrErrorBehavior emptyBehavior,
        SqlJsonQueryEmptyOrErrorBehavior errorBehavior,
        boolean jsonize) {
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
            return jsonQueryEmptyArray(jsonize);
          case EMPTY_OBJECT:
            return jsonQueryEmptyObject(jsonize);
          default:
            throw RESOURCE.illegalEmptyBehaviorInJsonQueryFunc(
                emptyBehavior.toString()).ex();
          }
        } else if (context.mode == PathMode.STRICT && isScalarObject(value)) {
          exc =
              RESOURCE.arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(
                  value.toString()).ex();
        } else {
          if (jsonize) {
            try {
              return jsonize(value);
            } catch (Exception e) {
              exc = e;
            }
          } else {
            return value;
          }
        }
      }
      switch (errorBehavior) {
      case ERROR:
        throw toUnchecked(requireNonNull(exc, "exc"));
      case NULL:
        return null;
      case EMPTY_ARRAY:
        return jsonQueryEmptyArray(jsonize);
      case EMPTY_OBJECT:
        return jsonQueryEmptyObject(jsonize);
      default:
        throw RESOURCE.illegalErrorBehaviorInJsonQueryFunc(
            errorBehavior.toString()).ex();
      }
    }

    private static Object jsonQueryEmptyArray(boolean jsonize) {
      return jsonize ? "[]" : Collections.emptyList();
    }

    private static String jsonQueryEmptyObject(boolean jsonize) {
      if (jsonize) {
        return "{}";
      } else {
        throw RESOURCE.illegalEmptyObjectInJsonQueryFunc().ex();
      }
    }
  }

  public static String jsonObject(SqlJsonConstructorNullClause nullClause,
      @Nullable Object... kvs) {
    assert kvs.length % 2 == 0;
    Map<String, @Nullable Object> map = new HashMap<>();
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

  public static void jsonObjectAggAdd(Map map, String k, @Nullable Object v,
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
      @Nullable Object... elements) {
    List<@Nullable Object> list = new ArrayList<>();
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

  public static void jsonArrayAggAdd(List list, @Nullable Object element,
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
      throw RESOURCE.exceptionWhileSerializingToJson(Objects.toString(input.obj)).ex(e);
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
      throw RESOURCE.invalidInputForJsonType(val.toString()).ex(ex);
    }
  }

  public static @Nullable Integer jsonDepth(String input) {
    return jsonDepth(jsonValueExpression(input));
  }

  public static @Nullable Integer jsonDepth(JsonValueContext input) {
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
      throw RESOURCE.invalidInputForJsonDepth(o.toString()).ex(ex);
    }
  }

  @SuppressWarnings("JdkObsolete")
  private static Integer calculateDepth(Object o) {
    if (isScalarObject(o)) {
      return 1;
    }
    // Note: even even though LinkedList implements Queue, it supports null values
    //
    Queue<Object> q = new LinkedList<>();
    int depth = 0;
    q.add(o);

    while (!q.isEmpty()) {
      int size = q.size();
      for (int i = 0; i < size; ++i) {
        Object obj = q.poll();
        if (obj instanceof Map) {
          q.addAll(((LinkedHashMap) obj).values());
        } else if (obj instanceof Collection) {
          q.addAll((Collection) obj);
        }
      }
      ++depth;
    }
    return depth;
  }

  public static @Nullable Integer jsonLength(String input) {
    return jsonLength(jsonApiCommonSyntax(input));
  }

  public static @Nullable Integer jsonLength(JsonValueContext input) {
    return jsonLength(jsonApiCommonSyntax(input));
  }

  public static @Nullable Integer jsonLength(String input, String pathSpec) {
    return jsonLength(jsonApiCommonSyntax(input, pathSpec));
  }

  public static @Nullable Integer jsonLength(JsonValueContext input, String pathSpec) {
    return jsonLength(jsonApiCommonSyntax(input, pathSpec));
  }

  public static @Nullable Integer jsonLength(JsonPathContext context) {
    final Integer result;
    final Object value;
    try {
      if (context.hasException()) {
        throw toUnchecked(requireNonNull(context.exc));
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
          context.toString()).ex(ex);
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
        throw toUnchecked(requireNonNull(context.exc));
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
          context.toString()).ex(ex);
    }
    return jsonize(list);
  }

  public static String jsonRemove(String input, String... pathSpecs) {
    return jsonRemove(jsonValueExpression(input), pathSpecs);
  }

  public static String jsonRemove(JsonValueContext input, String... pathSpecs) {
    try {
      DocumentContext ctx =
          JsonPath.parse(input.obj(),
              Configuration.builder()
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
          input.toString(), Arrays.toString(pathSpecs)).ex(ex);
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
      throw RESOURCE.invalidInputForJsonStorageSize(Objects.toString(input.obj)).ex(e);
    }
  }

  private static String jsonModify(JsonValueContext jsonDoc, JsonModifyMode type, Object... kvs) {
    int step = type == JsonModifyMode.REMOVE ? 1 : 2;
    assert kvs.length % step == 0;
    String result = null;
    DocumentContext ctx =
        JsonPath.parse(jsonDoc.obj(),
            Configuration.builder()
                .options(Option.SUPPRESS_EXCEPTIONS)
                .jsonProvider(JSON_PATH_JSON_PROVIDER)
                .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                .build());

    for (int i = 0; i < kvs.length; i += step) {
      String k = (String) kvs[i];
      Object v = kvs[i + step - 1];
      if (!(JsonPath.isPathDefinite(k) && k.contains(JSON_ROOT_PATH))) {
        throw RESOURCE.validationError(k).ex();
      }
      switch (type) {
      case REPLACE:
        if (k.equals(JSON_ROOT_PATH)) {
          result = jsonize(v);
        } else {
          if (ctx.read(k) != null) {
            ctx.set(k, v);
          }
        }
        break;
      case INSERT:
        if (!k.equals(JSON_ROOT_PATH) && ctx.read(k) == null) {
          insertToJson(ctx, k, v);
        }
        break;
      case SET:
        if (k.equals(JSON_ROOT_PATH)) {
          result = jsonize(v);
        } else {
          if (ctx.read(k) != null) {
            ctx.set(k, v);
          } else {
            insertToJson(ctx, k, v);
          }
        }
        break;
      case REMOVE:
        if (ctx.read(k) != null) {
          ctx.delete(k);
        }
        break;
      }
    }

    return result == null || result.isEmpty() ? ctx.jsonString() : result;
  }

  private static void insertToJson(DocumentContext ctx, String path, Object value) {
    final String parentPath;
    final String key;

    // The following paragraph of logic is mainly used to obtain the
    // parent node path of path and the key value that should be
    // inserted when the preant Path is map.
    // eg: path is $.a ,parentPath is $
    // eg: path is $.a[1] ,parentPath is $.a
    Integer dotIndex = path.lastIndexOf(".");
    Integer leftBracketIndex = path.lastIndexOf("[");
    if (dotIndex.equals(-1) && leftBracketIndex.equals(-1)) {
      parentPath = path;
      key = path;
    } else if (!dotIndex.equals(-1) && leftBracketIndex.equals(-1)) {
      parentPath = path.substring(0, dotIndex);
      key = path.substring(dotIndex + 1);
    } else if (dotIndex.equals(-1) && !leftBracketIndex.equals(-1)) {
      parentPath = path.substring(0, leftBracketIndex);
      key = path.substring(leftBracketIndex + 1);
    } else {
      int position = dotIndex > leftBracketIndex ? dotIndex : leftBracketIndex;
      parentPath = path.substring(0, position);
      key = path.substring(position);
    }

    Object obj = ctx.read(parentPath);
    if (obj instanceof Map) {
      ctx.put(parentPath, key, value.toString());
    } else if (obj instanceof Collection) {
      ctx.add(parentPath, value);
    }
  }

  public static String jsonReplace(String jsonDoc, Object... kvs) {
    return jsonReplace(jsonValueExpression(jsonDoc), kvs);
  }

  public static String jsonReplace(JsonValueContext input, Object... kvs) {
    try {
      return jsonModify(input, JsonModifyMode.REPLACE, kvs);
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonReplace(input.toString(), jsonize(kvs)).ex();
    }
  }

  public static String jsonInsert(String jsonDoc, Object... kvs) {
    return jsonInsert(jsonValueExpression(jsonDoc), kvs);
  }

  public static String jsonInsert(JsonValueContext input, Object... kvs) {
    try {
      return jsonModify(input, JsonModifyMode.INSERT, kvs);
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonInsert(input.toString(), jsonize(kvs)).ex();
    }
  }

  public static String jsonSet(String jsonDoc, Object... kvs) {
    return jsonSet(jsonValueExpression(jsonDoc), kvs);
  }

  public static String jsonSet(JsonValueContext input, Object... kvs) {
    try {
      return jsonModify(input, JsonModifyMode.SET, kvs);
    } catch (Exception ex) {
      throw RESOURCE.invalidInputForJsonSet(input.toString(), jsonize(kvs)).ex();
    }
  }

  public static @Nullable Boolean isJsonValue(@Nullable String input) {
    if (input == null) {
      return null;
    }
    try {
      dejsonize(input);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static @Nullable Boolean isJsonObject(@Nullable String input) {
    if (input == null) {
      return null;
    }
    try {
      Object o = dejsonize(input);
      return o instanceof Map;
    } catch (Exception e) {
      return false;
    }
  }

  public static @Nullable Boolean isJsonArray(@Nullable String input) {
    if (input == null) {
      return null;
    }
    try {
      Object o = dejsonize(input);
      return o instanceof Collection;
    } catch (Exception e) {
      return false;
    }
  }

  public static @Nullable Boolean isJsonScalar(@Nullable String input) {
    if (input == null) {
      return null;
    }
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
    public final @Nullable Object obj;
    public final @Nullable Exception exc;

    private JsonPathContext(@Nullable Object obj, @Nullable Exception exc) {
      this(PathMode.NONE, obj, exc);
    }

    private JsonPathContext(PathMode mode, @Nullable Object obj, @Nullable Exception exc) {
      assert obj == null || exc == null;
      this.mode = mode;
      this.obj = obj;
      this.exc = exc;
    }

    @EnsuresNonNullIf(expression = "exc", result = true)
    public boolean hasException() {
      return exc != null;
    }

    public static JsonPathContext withUnknownException(Exception exc) {
      return new JsonPathContext(PathMode.UNKNOWN, null, exc);
    }

    public static JsonPathContext withStrictException(Exception exc) {
      return new JsonPathContext(PathMode.STRICT, null, exc);
    }

    public static JsonPathContext withStrictException(String pathSpec, Exception exc) {
      if (exc.getClass() == InvalidPathException.class) {
        exc = RESOURCE.illegalJsonPathSpec(pathSpec).ex();
      }
      return withStrictException(exc);
    }

    public static JsonPathContext withJavaObj(PathMode mode, @Nullable Object obj) {
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
    public final @Nullable Object obj;
    public final @Nullable Exception exc;

    private JsonValueContext(@Nullable Object obj, @Nullable Exception exc) {
      assert obj == null || exc == null;
      this.obj = obj;
      this.exc = exc;
    }

    public static JsonValueContext withJavaObj(@Nullable Object obj) {
      return new JsonValueContext(obj, null);
    }

    public static JsonValueContext withException(Exception exc) {
      return new JsonValueContext(null, exc);
    }

    Object obj() {
      return requireNonNull(obj, "json object must not be null");
    }

    @EnsuresNonNullIf(expression = "exc", result = true)
    public boolean hasException() {
      return exc != null;
    }

    @Override public boolean equals(@Nullable Object o) {
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
      return jsonize(obj);
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

  /**
   * Used in the JsonModify function.
   */
  public enum JsonModifyMode {
    REPLACE,
    INSERT,
    SET,
    REMOVE
  }
}
