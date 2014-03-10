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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * <code>ShadowResourceBundle</code> is an abstract base class for
 * {@link ResourceBundle} classes which are backed by a properties file. When
 * the class is created, it loads a properties file with the same name as the
 * class.
 *
 * <p> In the standard scheme (see {@link ResourceBundle}),
 * if you call <code>{@link ResourceBundle#getBundle}("foo.MyResource")</code>,
 * it first looks for a class called <code>foo.MyResource</code>, then
 * looks for a file called <code>foo/MyResource.properties</code>. If it finds
 * the file, it creates a {@link PropertyResourceBundle} and loads the class.
 * The problem is if you want to load the <code>.properties</code> file
 * into a dedicated class; <code>ShadowResourceBundle</code> helps with this
 * case.
 *
 * <p> You should create a class as follows:<blockquote>
 *
 * <pre>package foo;
 *class MyResource extends org.eigenbase.resgen.ShadowResourceBundle {
 *    public MyResource() throws java.io.IOException {
 *    }
 *}</pre>
 *
 * </blockquote> Then when you call
 * {@link ResourceBundle#getBundle ResourceBundle.getBundle("foo.MyResource")},
 * it will find the class before the properties file, but still automatically
 * load the properties file based upon the name of the class.
 */
public abstract class ShadowResourceBundle extends ResourceBundle {
  private PropertyResourceBundle bundle;

  private static final ThreadLocal<Locale> MAP_THREAD_TO_LOCALE =
      new ThreadLocal<Locale>();
  protected static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  /**
   * Creates a <code>ShadowResourceBundle</code>, and reads resources from
   * a <code>.properties</code> file with the same name as the current class.
   * For example, if the class is called <code>foo.MyResource_en_US</code>,
   * reads from <code>foo/MyResource_en_US.properties</code>, then
   * <code>foo/MyResource_en.properties</code>, then
   * <code>foo/MyResource.properties</code>.
   *
   * @throws IOException on error
   */
  protected ShadowResourceBundle() throws IOException {
    super();
    Class clazz = getClass();
    InputStream stream = openPropertiesFile(clazz);
    if (stream == null) {
      throw new IOException("could not open properties file for " + getClass());
    }
    MyPropertyResourceBundle previousBundle =
        new MyPropertyResourceBundle(stream);
    bundle = previousBundle;
    stream.close();
    // Now load properties files for parent locales, which we deduce from
    // the names of our super-class, and its super-class.
    while (true) {
      clazz = clazz.getSuperclass();
      if (clazz == null
          || clazz == ShadowResourceBundle.class
          || !ResourceBundle.class.isAssignableFrom(clazz)) {
        break;
      }
      stream = openPropertiesFile(clazz);
      if (stream == null) {
        continue;
      }
      MyPropertyResourceBundle newBundle =
          new MyPropertyResourceBundle(stream);
      stream.close();
      if (previousBundle != null) {
        previousBundle.setParentTrojan(newBundle);
      } else {
        bundle = newBundle;
      }
      previousBundle = newBundle;
    }
  }

  /** Resource bundle based on properties. */
  static class MyPropertyResourceBundle extends PropertyResourceBundle {
    public MyPropertyResourceBundle(InputStream stream) throws IOException {
      super(stream);
    }

    void setParentTrojan(ResourceBundle parent) {
      super.setParent(parent);
    }
  }

  /**
   * Opens the properties file corresponding to a given class. The code is
   * copied from {@link ResourceBundle}.
   */
  private static InputStream openPropertiesFile(Class clazz) {
    final ClassLoader loader = clazz.getClassLoader();
    final String resName = clazz.getName().replace('.', '/') + ".properties";
    return (InputStream) java.security.AccessController.doPrivileged(
        new java.security.PrivilegedAction() {
          public Object run() {
            if (loader != null) {
              return loader.getResourceAsStream(resName);
            } else {
              return ClassLoader.getSystemResourceAsStream(resName);
            }
          }
        }
    );
  }

  public Enumeration<String> getKeys() {
    return bundle.getKeys();
  }

  protected Object handleGetObject(String key) {
    return bundle.getObject(key);
  }

  /**
   * Returns the instance of the <code>baseName</code> resource bundle for
   * the current thread's locale. For example, if called with
   * "mondrian.olap.MondrianResource", from a thread which has called {@link
   * #setThreadLocale}({@link Locale#FRENCH}), will get an instance of
   * "mondrian.olap.MondrianResource_FR" from the cache.
   *
   * <p> This method should be called from a derived class, with the proper
   * casting:<blockquote>
   *
   * <pre>class MyResource extends ShadowResourceBundle {
   *    ...
   *    /&#42;&#42;
   *      &#42; Retrieves the instance of {&#64;link MyResource} appropriate
   *      &#42; to the current locale. If this thread has specified a locale
   *      &#42; by calling {&#64;link #setThreadLocale}, this locale is used,
   *      &#42; otherwise the default locale is used.
   *      &#42;&#42;/
   *    public static MyResource instance() {
   *       return (MyResource) instance(MyResource.class.getName());
   *    }
   *    ...
   * }</pre></blockquote>
   *
   * @deprecated This method does not work correctly in dynamically
   * loaded jars.
   *
   * @param baseName Base name
   *
   * @return Resource bundle
   */
  protected static ResourceBundle instance(String baseName) {
    return instance(baseName, getThreadLocale());
  }
  /**
   * Returns the instance of the <code>baseName</code> resource bundle
   * for the given locale.
   *
   * <p> This method should be called from a derived class, with the proper
   * casting:<blockquote>
   *
   * <pre>class MyResource extends ShadowResourceBundle {
   *    ...
   *
   *    /&#42;&#42;
   *      &#42; Retrieves the instance of {&#64;link MyResource} appropriate
   *      &#42; to the given locale.
   *      &#42;&#42;/
   *    public static MyResource instance(Locale locale) {
   *       return (MyResource) instance(MyResource.class.getName(), locale);
   *    }
   *    ...
   * }</pre></blockquote>
   *
   * @param baseName Base name
   * @param locale Locale
   * @return Resource bundle
   *
   * @deprecated This method does not work correctly in dynamically
   * loaded jars.
   */
  protected static ShadowResourceBundle instance(
      String baseName, Locale locale) {
    if (locale == null) {
      locale = Locale.getDefault();
    }
    ResourceBundle bundle = ResourceBundle.getBundle(baseName, locale);
    return instance(baseName, locale, bundle);
  }

  /**
   * Returns the instance of the <code>baseName</code> resource bundle
   * for the given locale.
   *
   * <p> This method should be called from a derived class, with the proper
   * casting:<blockquote>
   *
   * <pre>class MyResource extends ShadowResourceBundle {
   *    ...
   *
   *    /&#42;&#42;
   *      &#42; Retrieves the instance of {&#64;link MyResource} appropriate
   *      &#42; to the given locale.
   *      &#42;&#42;/
   *    public static MyResource instance(Locale locale) {
   *       return (MyResource) instance(
   *           MyResource.class.getName(), locale,
   *           ResourceBundle.getBundle(MyResource.class.getName(), locale));
   *    }
   *    ...
   * }</pre></blockquote>
   *
   * @param baseName Base name
   * @param locale Locale
   * @param bundle Resource bundle
   * @return Resource bundle
   */
  protected static ShadowResourceBundle instance(
      String baseName, Locale locale, ResourceBundle bundle) {
    if (bundle instanceof PropertyResourceBundle) {
      throw new ClassCastException(
          "ShadowResourceBundle.instance('" + baseName + "','"
              + locale + "') found "
              + baseName + "_" + locale + ".properties but not "
              + baseName + "_" + locale + ".class");
    }
    return (ShadowResourceBundle) bundle;
  }

  /** Returns the preferred locale of the current thread, or
   * the default locale if the current thread has not called {@link
   * #setThreadLocale}.
   *
   * @return Locale */
  protected static Locale getThreadOrDefaultLocale() {
    Locale locale = getThreadLocale();
    if (locale == null) {
      return Locale.getDefault();
    } else {
      return locale;
    }
  }

  /** Sets the locale for the current thread. Used by {@link
   * #instance(String,Locale)}.
   *
   * @param locale Locale */
  public static void setThreadLocale(Locale locale) {
    MAP_THREAD_TO_LOCALE.set(locale);
  }

  /** Returns the preferred locale of the current thread, or null if the
   * thread has not called {@link #setThreadLocale}.
   *
   * @return Locale */
  public static Locale getThreadLocale() {
    return MAP_THREAD_TO_LOCALE.get();
  }
}

// End ShadowResourceBundle.java
