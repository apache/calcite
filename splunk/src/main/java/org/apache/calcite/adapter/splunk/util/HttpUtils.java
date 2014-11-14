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
package net.hydromatic.optiq.impl.splunk.util;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.*;

/**
 * Utilities for connecting to Splunk via HTTP.
 */
public class HttpUtils {
  private HttpUtils() {}

  private static final Logger LOGGER =
      StringUtils.getClassTracer(HttpUtils.class);

  public static HttpURLConnection getURLConnection(String url)
      throws IOException {
    URLConnection conn = new URL(url).openConnection();
    final HttpURLConnection httpConn = (HttpURLConnection) conn;

    // take care of https stuff - most of the time it's only needed to
    // secure client/server comm
    // not to establish the identity of the server
    if (httpConn instanceof HttpsURLConnection) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) httpConn;
      httpsConn.setSSLSocketFactory(
          TrustAllSslSocketFactory.createSSLSocketFactory());
      httpsConn.setHostnameVerifier(
          new HostnameVerifier() {
            public boolean verify(String arg0, SSLSession arg1) {
              return true;
            }
          });
    }

    return httpConn;
  }

  public static void appendURLEncodedArgs(
      StringBuilder out, Map<String, String> args) {
    int i = 0;
    try {
      for (Map.Entry<String, String> me : args.entrySet()) {
        if (i++ != 0) {
          out.append("&");
        }
        out.append(URLEncoder.encode(me.getKey(), "UTF-8"));
        out.append("=")
            .append(URLEncoder.encode(me.getValue(), "UTF-8"));
      }
    } catch (UnsupportedEncodingException ignore) {
      // ignore
    }
  }

  public static void appendURLEncodedArgs(
      StringBuilder out, CharSequence ... args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "args should contain an even number of items");
    }
    try {
      int appended = 0;
      for (int i = 0; i < args.length; i += 2) {
        if (args[i + 1] == null) {
          continue;
        }
        if (appended++ > 0) {
          out.append("&");
        }
        out.append(URLEncoder.encode(args[i].toString(), "UTF-8"))
            .append("=")
            .append(URLEncoder.encode(args[i + 1].toString(), "UTF-8"));
      }
    } catch (UnsupportedEncodingException ignore) {
      // ignore
    }
  }

  public static void close(Closeable c) {
    try {
      c.close();
    } catch (Exception ignore) {
      // ignore
    }
  }

  public static InputStream post(
      String url,
      CharSequence data,
      Map<String, String> headers) throws IOException {
    return post(url, data, headers, 10000, 60000);
  }

  public static InputStream post(
      String url,
      CharSequence data,
      Map<String, String> headers,
      int cTimeout,
      int rTimeout) throws IOException {
    return executeMethod("POST", url, data, headers, cTimeout, rTimeout);
  }

  public static InputStream executeMethod(
      String method, String url,
      CharSequence data, Map<String, String> headers,
      int ctimeout, int rtimeout) throws IOException {
    HttpURLConnection conn;
    OutputStreamWriter wr = null;

    try {
      conn = getURLConnection(url);
      conn.setRequestMethod(method);
      conn.setReadTimeout(rtimeout);
      conn.setConnectTimeout(ctimeout);

      if (headers != null) {
        for (Map.Entry<String, String> me : headers.entrySet()) {
          conn.setRequestProperty(me.getKey(), me.getValue());
        }
      }
      if (data != null) {
        conn.setDoOutput(true);
        wr = new OutputStreamWriter(conn.getOutputStream());
        wr.write(data.toString());
        wr.flush(); // Get the response
      }
      InputStream in = conn.getInputStream();
      wr.close();

      if (LOGGER.isLoggable(Level.FINE)) {
        LOGGER.fine("url: " + url + ", data: " + String.valueOf(data));
      }
      return in;
    } finally {
      close(wr);
    }
  }
}

// End HttpUtils.java
