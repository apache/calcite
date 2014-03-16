/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.resource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.Callable;

import org.eigenbase.util.EigenbaseContextException;
import org.eigenbase.util.Util;

import net.hydromatic.optiq.BuiltinMethod;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;

/**
 * Defining wrapper classes around resources that allow the compiler to check
 * whether the resources exist and have the argument types that your code
 * expects.
 */
public class Resources {
  private Resources() {}

  public static <T> T create(final String base, Class<T> clazz) {
    //noinspection unchecked
    return (T) Proxy.newProxyInstance(clazz.getClassLoader(),
        new Class[] {clazz},
        new InvocationHandler() {
          public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
            if (method.equals(BuiltinMethod.OBJECT_TO_STRING.method)) {
              return toString();
            }
            final Class<?> returnType = method.getReturnType();
            final Class[] types = {
              String.class, Locale.class, Method.class, Object[].class
            };
            final Constructor<?> constructor =
                returnType.getConstructor(types);
            return constructor.newInstance(
                base,
                ShadowResourceBundle.getThreadOrDefaultLocale(),
                method,
                args != null ? args : new Object[0]);
          }
        });
  }

  /** Applies all validations to all resource methods in the given
   * resource object. */
  public static void validate(Object o) {
    validate(o, EnumSet.allOf(Validation.class));
  }

  /** Applies the given validations to all resource methods in the given
   * resource object. */
  public static void validate(Object o, EnumSet<Validation> validations) {
    int count = 0;
    for (Method method : o.getClass().getMethods()) {
      if (!Modifier.isStatic(method.getModifiers())
          && Inst.class.isAssignableFrom(method.getReturnType())) {
        ++count;
        final Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] args = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
          args[i] = zero(parameterTypes[i]);
        }
        try {
          Inst inst = (Inst) method.invoke(o, args);
          inst.validate(validations);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("in " + method, e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException("in " + method, e.getCause());
        }
      }
    }
    if (count == 0 && validations.contains(Validation.AT_LEAST_ONE)) {
      throw new AssertionError("resource object " + o
          + " contains no resources");
    }
  }

  private static Object zero(Class<?> clazz) {
    return clazz == String.class ? ""
        : clazz == byte.class ? (byte) 0
        : clazz == char.class ? (char) 0
        : clazz == short.class ? (short) 0
        : clazz == int.class ? 0
        : clazz == long.class ? 0L
        : clazz == float.class ? 0F
        : clazz == double.class ? 0D
        : clazz == boolean.class ? false
        : null;
  }

  /** Resource instance. It contains the resource method (which
   * serves to identify the resource), the locale with which we
   * expect to render the resource, and any arguments. */
  public static class Inst {
    protected final String base;
    private final Locale locale;
    protected final Method method;
    protected final Object[] args;

    public Inst(String base, Locale locale, Method method, Object... args) {
      this.base = base;
      this.locale = locale;
      this.method = method;
      this.args = args;
    }

    @Override public boolean equals(Object obj) {
      return this == obj
          || obj != null
          && obj.getClass() == this.getClass()
          && locale == ((Inst) obj).locale
          && method == ((Inst) obj).method
          && Arrays.equals(args, ((Inst) obj).args);
    }

    @Override public int hashCode() {
      return Util.hashV(locale, method, Arrays.asList(args));
    }

    public ResourceBundle bundle() {
      return ResourceBundle.getBundle(base, locale);
    }

    public Inst localize(Locale locale) {
      return new Inst(base, locale, method, args);
    }

    public void validate(EnumSet<Validation> validations) {
      final ResourceBundle bundle = bundle();
      for (Validation validation : validations) {
        final String key = key();
        switch (validation) {
        case BUNDLE_HAS_RESOURCE:
          if (!bundle.containsKey(key)) {
            throw new AssertionError("key '" + key
                + "' not found for resource '" + method.getName()
                + "' in bundle '" + bundle + "'");
          }
          break;
        case MESSAGE_SPECIFIED:
          final BaseMessage annotation1 =
              method.getAnnotation(BaseMessage.class);
          if (annotation1 == null) {
            throw new AssertionError("resource '" + method.getName()
                + "' must specify BaseMessage");
          }
          break;
        case EVEN_QUOTES:
          String message = method.getAnnotation(BaseMessage.class).value();
          int numOfQuotes = CharMatcher.is('\'').countIn(message);
          if (numOfQuotes % 2 == 1) {
            throw new AssertionError("resource '" + method.getName()
                            + "' should have even number of quotes");
          }
          break;
        case MESSAGE_MATCH:
          final BaseMessage annotation2 =
              method.getAnnotation(BaseMessage.class);
          if (annotation2 != null) {
            final String value = annotation2.value();
            final String value2 = bundle.containsKey(key)
                ? bundle.getString(key)
                : null;
            if (!Util.equal(value, value2)) {
              throw new AssertionError("message for resource '"
                  + method.getName()
                  + "' is different between class and resource file");
            }
          }
          break;
        case ARGUMENT_MATCH:
          String raw = raw();
          MessageFormat format = new MessageFormat(raw);
          final Format[] formats = format.getFormatsByArgumentIndex();
          final List<Class> types = new ArrayList<Class>();
          final Class<?>[] parameterTypes = method.getParameterTypes();
          for (int i = 0; i < formats.length; i++) {
            Format format1 = formats[i];
            Class parameterType = parameterTypes[i];
            final Class<?> e;
            if (format1 instanceof NumberFormat) {
              e = parameterType == short.class
                  || parameterType == int.class
                  || parameterType == long.class
                  || parameterType == float.class
                  || parameterType == double.class
                  || Number.class.isAssignableFrom(parameterType)
                  ? parameterType
                  : Number.class;
            } else if (format1 instanceof DateFormat) {
              e = Date.class;
            } else {
              e = String.class;
            }
            types.add(e);
          }
          final List<Class<?>> parameterTypeList =
              Arrays.asList(parameterTypes);
          if (!types.equals(parameterTypeList)) {
            throw new AssertionError("type mismatch in method '"
                + method.getName() + "' between message format elements "
                + types + " and method parameters " + parameterTypeList);
          }
          break;
        }
      }
    }

    public String str() {
      String message = raw();
      MessageFormat format = new MessageFormat(message);
      format.setLocale(locale);
      return format.format(args);
    }

    public String raw() {
      try {
        return bundle().getString(key());
      } catch (MissingResourceException e) {
        // Resource is not in the bundle. (It is probably missing from the
        // .properties file.) Fall back to the base message.
        return method.getAnnotation(BaseMessage.class).value();
      }
    }

    private String key() {
      final Resource resource = method.getAnnotation(Resource.class);
      if (resource != null) {
        return resource.value();
      } else {
        final String name = method.getName();
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
      }
    }

    public Map<String, String> getProperties() {
      // At present, annotations allow at most one property per resource. We
      // could design new annotations if any resource needed more.
      final Property property = method.getAnnotation(Property.class);
      if (property == null) {
        return ImmutableMap.of();
      } else {
        return ImmutableMap.of(property.name(), property.value());
      }
    }
  }

  /** Sub-class of {@link Inst} that can throw an exception. Requires caused
   * by exception.*/
  public static class ExInstWithCause<T extends Exception> extends Inst {
    public ExInstWithCause(String base, Locale locale, Method method,
                           Object... args) {
      super(base, locale, method, args);
    }

    @Override public Inst localize(Locale locale) {
      return new ExInstWithCause<T>(base, locale, method, args);
    }

    public T ex(Throwable cause) {
      try {
        //noinspection unchecked
        final Class<T> exceptionClass = getExceptionClass();
        Constructor<T> constructor;
        final String str = str();
        boolean causeInConstructor = false;
        try {
          constructor = exceptionClass.getConstructor(String.class,
              Throwable.class
          );
          causeInConstructor = true;
        } catch (NoSuchMethodException nsmStringThrowable) {
          try {
            constructor = exceptionClass.getConstructor(String.class);
          } catch (NoSuchMethodException nsmString) {
            // Ignore nsmString to encourage users to have (String,
            // Throwable) constructors.
            throw nsmStringThrowable;
          }
        }
        if (causeInConstructor) {
          return constructor.newInstance(str, cause);
        }
        T ex = constructor.newInstance(str);
        if (cause != null) {
          try {
            ex.initCause(cause);
          } catch (IllegalStateException iae) {
            // Sorry, unable to add cause via constructor and via initCause
          }
        }
        return ex;
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof Error) {
          throw (Error) e.getCause();
        } else if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          throw new RuntimeException(e);
        }
      }
    }

    private Class<T> getExceptionClass() {
      // Get exception type from ExInstWithCause<MyException> type parameter
      // ExInstWithCause might be one of super classes.
      // That is why we need findTypeParameters to find ExInstWithCause in
      // superclass chain

      Type type = method.getGenericReturnType();
      TypeFactory typeFactory = TypeFactory.defaultInstance();
      JavaType[] args = typeFactory.findTypeParameters(
          typeFactory.constructType(type), ExInstWithCause.class);
      if (args == null) {
        throw new IllegalStateException("Unable to find superclass"
            + " ExInstWithCause for " + type);
      }
      // Update this if ExInstWithCause gets more type parameters
      // For instance if B and C are added ExInstWithCause<A, B, C>,
      // code below should be updated
      if (args.length != 1) {
        throw new IllegalStateException("ExInstWithCause should have"
            + " exactly one type parameter");
      }
      return (Class<T>) args[0].getRawClass();
    }

    protected void validateException(Callable<Exception> exSupplier) {
      Throwable cause = null;
      try {
        //noinspection ThrowableResultOfMethodCallIgnored
        final Exception ex = exSupplier.call();
        if (ex == null) {
          cause = new NullPointerException();
        }
      } catch (AssertionError e) {
        cause = e;
      } catch (RuntimeException e) {
        cause = e;
      } catch (Exception e) {
        // This can never happen since exSupplier should be just a ex() call.
        // catch(Exception) is required since Callable#call throws Exception.
        // Just in case we get exception somehow, we will rethrow it as a part
        // of AssertionError below.
        cause = e;
      }
      if (cause != null) {
        AssertionError assertionError = new AssertionError(
            "error instantiating exception for resource '"
            + method.getName() + "'");
        assertionError.initCause(cause);
        throw assertionError;
      }
    }
    @Override
    public void validate(EnumSet<Validation> validations) {
      super.validate(validations);
      if (validations.contains(Validation.CREATE_EXCEPTION)) {
        validateException(new Callable<Exception>() {
          public Exception call() throws Exception {
            return ex(new NullPointerException("test"));
          }
        });
      }
    }
  }

  /** Sub-class of {@link Inst} that can throw an exception without caused by.*/
  public static class ExInst<T extends Exception> extends ExInstWithCause<T> {
    public ExInst(String base, Locale locale, Method method, Object... args) {
      super(base, locale, method, args);
    }

    public T ex() {
      return ex(null);
    }

    @Override
    public void validate(EnumSet<Validation> validations) {
      super.validate(validations);
      if (validations.contains(Validation.CREATE_EXCEPTION)) {
        validateException(new Callable<Exception>() {
          public Exception call() throws Exception {
            return ex();
          }
        });
      }
    }
  }

  /** SQL language feature. Expressed as the exception that would be
   * thrown if it were used while disabled. */
  public static class Feature extends
      ExInstWithCause<EigenbaseContextException> {
    public Feature(String base, Locale locale, Method method, Object... args) {
      super(base, locale, method, args);
    }
  }

  /** Types of validation that can be performed on a resource. */
  public enum Validation {
    /** Checks that each method's resource key corresponds to a resource in the
     * bundle. */
    BUNDLE_HAS_RESOURCE,

    /** Checks that there is at least one resource in the bundle. */
    AT_LEAST_ONE,

    /** Checks that the base message annotation is on every resource. */
    MESSAGE_SPECIFIED,

    /** Checks that every message contains even number of quotes. */
    EVEN_QUOTES,

    /** Checks that the base message matches the message in the bundle. */
    MESSAGE_MATCH,

    /** Checks that it is possible to create an exception. */
    CREATE_EXCEPTION,

    /** Checks that the parameters of the method are consistent with the
     * format elements in the base message. */
    ARGUMENT_MATCH,
  }

  /** The message in the default locale. */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface BaseMessage {
    String value();
  }

  /** The name of the property in the resource file. */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Resource {
    String value();
  }

  /** Property of a resource. */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Property {
    String name();
    String value();
  }
}

// End Resources.java
