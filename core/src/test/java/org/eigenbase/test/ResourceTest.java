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

import java.util.EnumSet;

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

  @Test public void testValidateReturnType() {
    try {
      Resources.validate(fooResource, EnumSet.of(Validation.RETURN_TYPE));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo(
              "resource 'shouldReturnExInst' has ExceptionClass, so return should be ExInst<class java.lang.IllegalArgumentException>"));
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

  @Test public void testValidateMessageMatch() {
    try {
      Resources.validate(fooResource, EnumSet.of(Validation.MESSAGE_MATCH));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo(
              "message for resource 'differentMessageInPropertiesFile' is different between class and resource file"));
    }
  }

  @Test public void testValidateExceptionClassSpecified() {
    try {
      Resources.validate(fooResource,
          EnumSet.of(Validation.EXCEPTION_CLASS_SPECIFIED));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo(
              "resource 'exceptionClassNotSpecified' returns ExInst so must specify ExceptionClass"));
    }
  }

  @Test public void testValidateCreateException() {
    try {
      Resources.validate(fooResource, EnumSet.of(Validation.CREATE_EXCEPTION));
      fail("should have thrown");
    } catch (AssertionError e) {
      assertThat(e.getMessage(),
          equalTo("error instantiating exception for resource 'myException'"));
      assertThat(e.getCause().getMessage(),
          equalTo(
              "java.lang.NoSuchMethodException: org.eigenbase.test.ResourceTest$MyException.<init>(java.lang.String, java.lang.Throwable)"));
    }
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
   * a (String, Throwable) constructor. */
  public static class MyException extends RuntimeException {
    public MyException() {
      super();
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
    @ExceptionClass(IllegalArgumentException.class)
    ExInst<IllegalArgumentException> illArg(String s);

    @BaseMessage("has ExceptionClass so should return exinst")
    @ExceptionClass(IllegalArgumentException.class)
    Inst shouldReturnExInst();

    @BaseMessage("should return inst")
    String shouldReturnInst();

    @BaseMessage("exception class not specified")
    ExInst<MyException> exceptionClassNotSpecified();

    @BaseMessage("exception isn't throwable")
    @ExceptionClass(MyException.class)
    ExInst<MyException> myException();

    @BaseMessage("argument {0} does not match {1,number,#}")
    Inst mismatchedArguments(String s, int i, String s2);
  }
}
