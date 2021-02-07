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
package org.apache.calcite.util;

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** Utilities for creating immutable beans. */
public class ImmutableBeans {
  /** Cache of method handlers of each known class, because building a set of
   * handlers is too expensive to do each time we create a bean.
   *
   * <p>The cache uses weak keys so that if a class is unloaded, the cache
   * entry will be removed. */
  private static final LoadingCache<Class, Def> CACHE =
      CacheBuilder.newBuilder()
          .weakKeys()
          .softValues()
          .build(new CacheLoader<Class, Def>() {
            @Override public Def load(Class key) {
              //noinspection unchecked
              return makeDef(key);
            }
          });

  private ImmutableBeans() {}

  /** Creates an immutable bean that implements a given interface. */
  public static <T extends Object> T create(Class<T> beanClass) {
    return create_(beanClass, ImmutableMap.of());
  }

  /** Creates a bean of a given class whose contents are the same as this bean.
   *
   * <p>You typically use this to downcast a bean to a sub-class. */
  public static <T extends Object> T copy(Class<T> beanClass, Object o) {
    final BeanImpl<?> bean = (BeanImpl) Proxy.getInvocationHandler(o);
    return create_(beanClass, bean.map);
  }

  private static <T extends Object> T create_(Class<T> beanClass,
      ImmutableMap<String, Object> valueMap) {
    if (!beanClass.isInterface()) {
      throw new IllegalArgumentException("must be interface");
    }
    try {
      @SuppressWarnings("unchecked")
      final Def<T> def =
          (Def) CACHE.get(beanClass);
      return def.makeBean(valueMap);
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  private static <T extends Object> Def<T> makeDef(Class<T> beanClass) {
    final ImmutableSortedMap.Builder<String, Class> propertyNameBuilder =
        ImmutableSortedMap.naturalOrder();
    final ImmutableMap.Builder<Method, Handler<T>> handlers =
        ImmutableMap.builder();
    final Set<String> requiredPropertyNames = new HashSet<>();
    final Set<String> copyPropertyNames = new HashSet<>();

    // First pass, add "get" methods and build a list of properties.
    for (Method method : beanClass.getMethods()) {
      if (!Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      final Property property = method.getAnnotation(Property.class);
      if (property == null) {
        continue;
      }
      final Mode mode;
      final Object defaultValue = getDefault(method);
      final String methodName = method.getName();
      final String propertyName;
      if (methodName.startsWith("get")) {
        propertyName = methodName.substring("get".length());
        mode = Mode.GET;
      } else if (methodName.startsWith("is")) {
        propertyName = methodName.substring("is".length());
        mode = Mode.GET;
      } else if (methodName.startsWith("with")) {
        continue;
      } else {
        propertyName = methodName.substring(0, 1).toUpperCase(Locale.ROOT)
            + methodName.substring(1);
        mode = Mode.GET;
      }
      final Class<?> propertyType = method.getReturnType();
      if (method.getParameterCount() > 0) {
        throw new IllegalArgumentException("method '" + methodName
            + "' has too many parameters");
      }
      final boolean required = propertyType.isPrimitive()
          || !hasAnnotation(
              method.getAnnotatedReturnType(),
          "org.checkerframework.checker.nullness.qual.Nullable");
      if (required) {
        requiredPropertyNames.add(propertyName);
      }
      final boolean copy = property.makeImmutable()
          && (ReflectUtil.mightBeAssignableFrom(propertyType, Collection.class)
              || ReflectUtil.mightBeAssignableFrom(propertyType, Map.class));
      if (copy) {
        copyPropertyNames.add(propertyName);
      }
      propertyNameBuilder.put(propertyName, propertyType);
      final Object defaultValue2 =
          convertDefault(defaultValue, propertyName, propertyType);
      handlers.put(method, (bean, args) -> {
        switch (mode) {
        case GET:
          final Object v = bean.map.get(propertyName);
          if (v != null) {
            return v;
          }
          if (required && defaultValue == null) {
            throw new IllegalArgumentException("property '" + beanClass.getName()
                + "#" + propertyName
                + "' is required and has no default value");
          }
          return defaultValue2;
        default:
          throw new AssertionError();
        }
      });
    }

    // Second pass, add "with" methods if they correspond to a property.
    final ImmutableMap<String, Class> propertyNames =
        propertyNameBuilder.build();
    for (Method method : beanClass.getMethods()) {
      if (!Modifier.isPublic(method.getModifiers())
          || method.isDefault()) {
        continue;
      }
      final Property property = method.getAnnotation(Property.class);
      final Mode mode;
      final String propertyName;
      final String methodName = method.getName();
      if (methodName.startsWith("get")) {
        continue;
      } else if (methodName.startsWith("is")) {
        continue;
      } else if (property != null) {
        // If there is a property annotation, treat this as a getter. For
        // example, there could be a property "set", with getter method
        // "Set<String> set()" and setter method "Bean withSet(Set<String>)".
        continue;
      } else if (methodName.startsWith("with")) {
        propertyName = methodName.substring("with".length());
        mode = Mode.WITH;
      } else if (methodName.startsWith("set")) {
        propertyName = methodName.substring("set".length());
        mode = Mode.SET;
      } else {
        continue;
      }
      final Class propertyClass = propertyNames.get(propertyName);
      if (propertyClass == null) {
        throw new IllegalArgumentException("cannot find property '"
            + propertyName + "' for method '" + methodName
            + "'; maybe add a method 'get" + propertyName + "'?'");
      }
      switch (mode) {
      case WITH:
        if (method.getReturnType() != beanClass
            && method.getReturnType() != method.getDeclaringClass()) {
          throw new IllegalArgumentException("method '" + methodName
              + "' should return the bean class '" + beanClass
              + "', actually returns '" + method.getReturnType() + "'");
        }
        break;
      case SET:
        if (method.getReturnType() != void.class) {
          throw new IllegalArgumentException("method '" + methodName
              + "' should return void, actually returns '"
              + method.getReturnType() + "'");
        }
        break;
      default:
        break;
      }
      if (method.getParameterCount() != 1) {
        throw new IllegalArgumentException("method '" + methodName
            + "' should have one parameter, actually has "
            + method.getParameterCount());
      }
      final Class propertyType = propertyNames.get(propertyName);
      if (!method.getParameterTypes()[0].equals(propertyType)) {
        throw new IllegalArgumentException("method '" + methodName
            + "' should have parameter of type " + propertyType
            + ", actually has " + method.getParameterTypes()[0]);
      }
      final boolean required = requiredPropertyNames.contains(propertyName);
      final boolean copy = copyPropertyNames.contains(propertyName);
      handlers.put(method, (bean, args) -> {
        switch (mode) {
        case WITH:
          final Object v = bean.map.get(propertyName);
          final ImmutableMap.Builder<String, Object> mapBuilder;
          if (v != null) {
            if (v.equals(args[0])) {
              return bean.asBean();
            }
            // the key already exists; painstakingly copy all entries
            // except the one with this key
            mapBuilder = ImmutableMap.builder();
            bean.map.forEach((key, value) -> {
              if (!key.equals(propertyName)) {
                mapBuilder.put(key, value);
              }
            });
          } else {
            // the key does not exist; put the whole map into the builder
            mapBuilder = ImmutableMap.<String, Object>builder()
                .putAll(bean.map);
          }
          if (args[0] != null) {
            mapBuilder.put(propertyName, value(copy, args[0]));
          } else {
            if (required) {
              throw new IllegalArgumentException("cannot set required "
                  + "property '" + propertyName + "' to null");
            }
          }
          final ImmutableMap<String, Object> map = mapBuilder.build();
          return bean.withMap(map).asBean();
        default:
          throw new AssertionError();
        }
      });
    }

    // Third pass, add default methods.
    for (Method method : beanClass.getMethods()) {
      if (method.isDefault()) {
        final MethodHandle methodHandle;
        try {
          methodHandle = Compatible.INSTANCE.lookupPrivate(beanClass)
              .unreflectSpecial(method, beanClass);
        } catch (Throwable throwable) {
          throw new RuntimeException("while binding method " + method,
              throwable);
        }
        handlers.put(method, (bean, args) -> {
          try {
            return methodHandle.bindTo(bean.asBean())
                .invokeWithArguments(args);
          } catch (RuntimeException | Error e) {
            throw e;
          } catch (Throwable throwable) {
            throw new RuntimeException("while invoking method " + method,
                throwable);
          }
        });
      }
    }

    handlers.put(getMethod(Object.class, "toString"),
        (bean, args) -> new TreeMap<>(bean.map).toString());
    handlers.put(getMethod(Object.class, "hashCode"),
        (bean, args) -> new TreeMap<>(bean.map).hashCode());
    handlers.put(getMethod(Object.class, "equals", Object.class),
        (bean, args) -> bean == args[0]
            // Use a little arg-swap trick because it's difficult to get inside
            // a proxy
            || beanClass.isInstance(args[0])
            && args[0].equals(bean.map)
            // Strictly, a bean should not equal a Map but it's convenient
            || args[0] instanceof Map
            && bean.map.equals(args[0]));
    return new Def<>(beanClass, handlers.build());
  }

  /** Returns the value to be stored, optionally copying. */
  private static Object value(boolean copy, Object o) {
    if (copy) {
      if (o instanceof List) {
        return ImmutableNullableList.copyOf((List) o);
      }
      if (o instanceof Set) {
        return ImmutableNullableSet.copyOf((Set) o);
      }
      if (o instanceof Map) {
        return ImmutableNullableMap.copyOf((Map) o);
      }
    }
    return o;
  }

  /** Looks for an annotation by class name.
   * Useful if you don't want to depend on the class
   * (e.g. "org.checkerframework.checker.nullness.qual.Nullable") at compile time. */
  private static boolean hasAnnotation(AnnotatedElement element, String className) {
    for (Annotation annotation : element.getDeclaredAnnotations()) {
      if (annotation.annotationType().getName().equals(className)) {
        return true;
      }
    }
    return false;
  }

  private static @Nullable Object getDefault(Method method) {
    Object defaultValue = null;
    final IntDefault intDefault = method.getAnnotation(IntDefault.class);
    if (intDefault != null) {
      defaultValue = intDefault.value();
    }
    final BooleanDefault booleanDefault =
        method.getAnnotation(BooleanDefault.class);
    if (booleanDefault != null) {
      defaultValue = booleanDefault.value();
    }
    final StringDefault stringDefault =
        method.getAnnotation(StringDefault.class);
    if (stringDefault != null) {
      defaultValue = stringDefault.value();
    }
    final EnumDefault enumDefault =
        method.getAnnotation(EnumDefault.class);
    if (enumDefault != null) {
      defaultValue = enumDefault.value();
    }
    return defaultValue;
  }

  private static @Nullable Object convertDefault(@Nullable Object defaultValue, String propertyName,
      Class<?> propertyType) {
    if (propertyType.equals(SqlConformance.class)) {
      // Workaround for SqlConformance because it is actually not a Enum.
      propertyType = SqlConformanceEnum.class;
    }
    if (defaultValue == null || !propertyType.isEnum()) {
      return defaultValue;
    }
    // checkerframework does not infer "isEnum" here, so castNonNull
    for (Object enumConstant : castNonNull(propertyType.getEnumConstants())) {
      if (((Enum) enumConstant).name().equals(defaultValue)) {
        return enumConstant;
      }
    }
    throw new IllegalArgumentException("property '" + propertyName
        + "' is an enum but its default value " + defaultValue
        + " is not a valid enum constant");
  }

  private static Method getMethod(Class<Object> aClass,
      String methodName, Class... parameterTypes) {
    try {
      return aClass.getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new AssertionError();
    }
  }

  /** Whether the method is reading or writing. */
  private enum Mode {
    GET, SET, WITH
  }

  /** Handler for a particular method call; called with "this" and arguments.
   *
   * @param <T> Bean type */
  private interface Handler<T extends Object> {
    @Nullable Object apply(BeanImpl<T> bean, @Nullable Object[] args);
  }

  /** Property of a bean. Apply this annotation to the "get" method. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface Property {
    /** Whether to make immutable copies of property values. */
    boolean makeImmutable() default true;
  }

  /** Default value of an int property. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface IntDefault {
    int value();
  }

  /** Default value of a boolean property of a bean. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface BooleanDefault {
    boolean value();
  }

  /** Default value of a String property of a bean. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface StringDefault {
    String value();
  }

  /** Default value of an enum property of a bean. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface EnumDefault {
    String value();
  }

  /** Default value of a String or enum property of a bean that is null. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public @interface NullDefault {
  }

  /** Implementation of an instance of a bean; stores property
   * values in a map, and also implements {@code InvocationHandler}
   * so that it can retrieve calls from a reflective proxy.
   *
   * @param <T> Bean type */
  private static class BeanImpl<T extends Object> implements InvocationHandler {
    private final Def<T> def;
    private final ImmutableMap<String, Object> map;

    BeanImpl(Def<T> def, ImmutableMap<String, Object> map) {
      this.def = Objects.requireNonNull(def, "def");
      this.map = Objects.requireNonNull(map, "map");
    }

    @Override public @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args) {
      final Handler handler = def.handlers.get(method);
      if (handler == null) {
        throw new IllegalArgumentException("no handler for method " + method);
      }
      return handler.apply(this, args);
    }

    /** Returns a copy of this bean that has a different map. */
    BeanImpl<T> withMap(ImmutableMap<String, Object> map) {
      return new BeanImpl<>(def, map);
    }

    /** Wraps this handler in a proxy that implements the required
     * interface. */
    T asBean() {
      return def.beanClass.cast(
          Proxy.newProxyInstance(def.beanClass.getClassLoader(),
              new Class[] {def.beanClass}, this));
    }
  }

  /** Definition of a bean. Consists of its class and handlers.
   *
   * @param <T> Class of bean */
  private static class Def<T extends Object> {
    private final Class<T> beanClass;
    private final ImmutableMap<Method, Handler<T>> handlers;

    Def(Class<T> beanClass, ImmutableMap<Method, Handler<T>> handlers) {
      this.beanClass = Objects.requireNonNull(beanClass, "beanClass");
      this.handlers = Objects.requireNonNull(handlers, "handlers");
    }

    private T makeBean(ImmutableMap<String, Object> map) {
      return new BeanImpl<>(this, map).asBean();
    }
  }
}
