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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.text.DateFormat;
import java.text.Format;
import java.text.MessageFormat;
import java.text.NumberFormat;

import org.eigenbase.resgen.*;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

/**
 * Tool to convert EigenbaseResource.xml into NewEigenbaseResource.java.
 * A one-time conversion task - not intended to be a generic tool.
 */
public class ResourceMigrate {
  private ResourceMigrate() {}

  public static void main(String[] args) {
    try {
      main0();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  static void main0() throws IOException {
    final URL url =
        new URL(
            "file:/Users/jhyde/open1/optiq/core/src/main/java/org/eigenbase/resource/EigenbaseResource.xml");
    final ResourceDef.ResourceBundle resourceBundle = load(url);
    File outFile = new File("Out.java");
    final FileWriter fileWriter = new FileWriter(outFile);
    final PrintWriter pw = new PrintWriter(fileWriter);
    for (ResourceDef.Resource resource : resourceBundle.resources) {
      pw.println("    @BaseMessage(\"" + getMessage(resource).replace("\"",
          "\\\"") + "\")");
      if (resource.properties.length > 0) {
        assert resource.properties.length == 1; // can't handle more than 1
        final ResourceDef.Property property = resource.properties[0];
        pw.println("    @Property(name = \"" + property.name + "\", value = \""
            + property.cdata + "\")");
      }
      if (resource instanceof ResourceDef.Exception) {
        pw.println("    @ExceptionClass("
            + exceptionClassName((ResourceDef.Exception) resource, true)
            + ".class)");
      }
      pw.println("    "
          + returnType(resource)
          + " " + Character.toLowerCase(resource.name.charAt(0))
          + resource.name.substring(1)
          + "(" + getParameterList(resource)
          + ");");
      pw.println();
    }
    pw.close();
  }

  private static String getMessage(ResourceDef.Resource resource) {
    return resource.text.cdata.replaceAll("\n *",
        " ");
  }

  private static String returnType(ResourceDef.Resource resource) {
    if (resource instanceof ResourceDef.Exception) {
      return "ExInst<"
          + exceptionClassName((ResourceDef.Exception) resource, true) + ">";
    }
    return "Inst";
  }

  private static String exceptionClassName(ResourceDef.Exception resource,
      boolean simple) {
    String className = resource.className;
    if (className == null) {
      className = "EigenbaseException";
    }
    if (simple && className.contains(".")) {
      className = className.substring(className.lastIndexOf('.'));
    }
    return className;
  }

  private static String getParameterList(ResourceDef.Resource resource) {
    final StringBuilder buf = new StringBuilder();
    final String text = resource.text.cdata;
    final Format[] formats = new MessageFormat(text)
        .getFormatsByArgumentIndex();
    for (int i = 0; i < formats.length; i++) {
      Format format = formats[i];
      if (i > 0) {
        buf.append(", ");
      }
      if (format instanceof NumberFormat) {
        buf.append("int ");
      } else if (format instanceof DateFormat) {
        buf.append("java.util.Date ");
      } else {
        buf.append("String ");
      }
      buf.append("a").append(i);
    }
    return buf.toString();
  }

  static ResourceDef.ResourceBundle load(URL url)
    throws IOException {
    return load(url.openStream());
  }

  /** loads InputStream and returns set of resources */
  static ResourceDef.ResourceBundle load(InputStream inStream)
    throws IOException {
    try {
      Parser parser = XOMUtil.createDefaultParser();
      DOMWrapper def = parser.parse(inStream);
      return new ResourceDef.ResourceBundle(def);
    } catch (XOMException err) {
      throw new IOException(err.toString());
    }
  }

}

// End ResourceMigrate.java
