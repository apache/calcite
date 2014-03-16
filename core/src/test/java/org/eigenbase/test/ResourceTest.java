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
package org.eigenbase.test;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.Locale;

import org.eigenbase.resource.Resources;

import org.junit.Test;

import static org.eigenbase.resource.Resources.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Tests for the {@link Resources} framework.
 */
public class ResourceTest {
  final FooResource fooResource =
      Resources.create("org.eigenbase.test.ResourceTest", FooResource.class);

  @Test public void testSimple() {
    assertThat(fooResource.helloWorld().str(), equalTo("hello, world!"));
    assertThat(fooResource.differentMessageInPropertiesFile().str(),
        equalTo("message in properties file"));
    assertThat(fooResource.onlyInClass().str(),
        equalTo("only in class"));
    assertThat(fooResource.onlyInPropertiesFile().str(),
        equalTo("message in properties file"));
  }

  @Test public void testProperty() {
    assertThat(fooResource.helloWorld().getProperties().size(), equalTo(0));
    assertThat(fooResource.withProperty(0).str(), equalTo("with properties 0"));
    assertThat(fooResource.withProperty(0).getProperties().size(), equalTo(1));
    assertThat(fooResource.withProperty(0).getProperties().get("prop"),
        equalTo("my value"));
    assertThat(fooResource.withProperty(1000).getProperties().get("prop"),
        equalTo("my value"));
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Test public void testException() {
    assertThat(fooResource.illArg("xyz").ex().getMessage(),
        equalTo("bad arg xyz"));
    assertThat(fooResource.illArg("xyz").ex().getCause(), nullValue());
    final Throwable npe = new NullPointerException();
    assertThat(fooResource.illArg("").ex(npe).getCause(), equalTo(npe));
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Test public void testSuperChainException() {
    assertThat(fooResource.exceptionSuperChain().ex().getMessage(),
        equalTo("super chain exception"));
    assertThat(fooResource.exceptionSuperChain().ex().getClass().getName(),
        equalTo(IllegalStateException.class.getName()));
  }

  /** Tests that get validation error if bundle does not contain resource. */
  @Test public void testValidateBundleHasResource() {
    try {
      Resources.validate(fooResource,
          EnumSet.of(Validation.BUNDLE_HAS_RESOURCE));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          startsWith(
              "key 'OnlyInClass' not found for resource 'onlyInClass' in bundle 'java.util.PropertyResourceBundle@"));
    }
  }

  @Test public void testValidateAtLeastOne() {
    // succeeds - has several resources
    Resources.validate(fooResource, EnumSet.of(Validation.AT_LEAST_ONE));

    // fails validation - has no resources
    try {
      Resources.validate("foo", EnumSet.of(Validation.AT_LEAST_ONE));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("resource object foo contains no resources"));
    }
  }

  @Test public void testValidateMessageSpecified() {
    try {
      Resources.validate(fooResource, EnumSet.of(Validation.MESSAGE_SPECIFIED));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("resource 'onlyInPropertiesFile' must specify BaseMessage"));
    }
  }

  @Test public void testValidateMessageMatchDifferentMessageInPropertiesFile() {
    try {
      fooResource.differentMessageInPropertiesFile().validate(
          EnumSet.of(Validation.MESSAGE_MATCH));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo(
              "message for resource 'differentMessageInPropertiesFile' is different between class and resource file"));
    }
  }

  @Test public void testValidateOddQuotes() {
    try {
      fooResource.oddQuotes().validate(EnumSet.of(Validation.EVEN_QUOTES));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("resource 'oddQuotes' should have even number of quotes"));
    }
  }

  @Test public void testValidateCreateException() {
    try {
      fooResource.myException().validate(EnumSet.of(Validation.CREATE_EXCEPTION)
      );
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("error instantiating exception for resource 'myException'"));
      assertThat(e.getCause().getMessage(),
          equalTo(
              "java.lang.NoSuchMethodException: org.eigenbase.test.ResourceTest$MyException.<init>(java.lang.String, java.lang.Throwable)"));
    }
  }

  @Test public void testValidateCauselessFail() {
    try {
      fooResource.causelessFail().validate(
          EnumSet.of(Validation.CREATE_EXCEPTION)
      );
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("error instantiating exception for resource "
              + "'causelessFail'"));
      assertThat(e.getCause().getMessage(),
          equalTo(
              "Cause is required, message = can't be used causeless"));
    }
  }

  @Test public void testValidateExceptionWithCause() {
    fooResource.exceptionWithCause().validate(
        EnumSet.of(Validation.CREATE_EXCEPTION)
    );
  }

  @Test public void testValidateMatchArguments() {
    try {
      Resources.validate(fooResource, EnumSet.of(Validation.ARGUMENT_MATCH));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo(
              "type mismatch in method 'mismatchedArguments' between message format elements [class java.lang.String, int] and method parameters [class java.lang.String, int, class java.lang.String]"));
    }
  }

  // TODO: check that each resource in the bundle is used by precisely
  //  one method

  /** Exception that cannot be thrown by {@link ExInst} because it does not have
   * a (String, Throwable) constructor, nor does it have a (String)
   * constructor. */
  public static class MyException extends RuntimeException {
    public MyException() {
      super();
    }
  }

  /** Abstract class used to test identification of exception classes via
   * superclass chains */
  public abstract static class MyExInst<W extends Exception> extends
      ExInst<W> {
    public MyExInst(String base, Locale locale, Method method, Object... args) {
      super(base, locale, method, args);
    }
  }

  /** Subtype of ExInst, however exception type is not directly
   * passed to ExInst. The test must still detect the correct class. */
  public static class MyExInstImpl extends MyExInst<IllegalStateException> {
    public MyExInstImpl(String base, Locale locale, Method method,
                        Object... args) {
      super(base, locale, method, args);
    }
  }

  /** Exception that always requires cause
   */
  public static class MyExceptionRequiresCause extends RuntimeException {
    public MyExceptionRequiresCause(String message, Throwable cause) {
      super(message, cause);
      if (cause == null) {
        throw new IllegalArgumentException("Cause is required, "
            + "message = " + message);
      }
    }
  }

  /** A resource object to be tested. Has one of each flaw. */
  public interface FooResource {
    @BaseMessage("hello, world!")
    Inst helloWorld();

    @BaseMessage("message in class")
    Inst differentMessageInPropertiesFile();

    @BaseMessage("only in class")
    Inst onlyInClass();

    Inst onlyInPropertiesFile();

    @BaseMessage("with properties {0,number}")
    @Property(name = "prop", value = "my value")
    Inst withProperty(int x);

    @BaseMessage("bad arg {0}")
    ExInst<IllegalArgumentException> illArg(String s);

    @BaseMessage("should return inst")
    String shouldReturnInst();

    @BaseMessage("exception isn''t throwable")
    ExInst<MyException> myException();

    @BaseMessage("Can't use odd quotes")
    Inst oddQuotes();

    @BaseMessage("can''t be used causeless")
    ExInst<MyExceptionRequiresCause> causelessFail();

    @BaseMessage("should work since cause is provided")
    ExInstWithCause<MyExceptionRequiresCause> exceptionWithCause();

    @BaseMessage("argument {0} does not match {1,number,#}")
    Inst mismatchedArguments(String s, int i, String s2);

    @BaseMessage("super chain exception")
    MyExInstImpl exceptionSuperChain();
  }
}
