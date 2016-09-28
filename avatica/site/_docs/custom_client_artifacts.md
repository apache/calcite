---
layout: docs
title: Custom Client Artifacts
permalink: /docs/custom_client_artifacts.html
---

<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

As of Apache Calcite Avatica 1.9.0, there are two artifacts (jars) provided that enable client-access
to an Avatica server over JDBC.

{% highlight xml %}
<dependencies>
  <!-- Shaded artifact -->
  <dependency>
    <groupId>org.apache.calcite.avatica</groupId>
    <artifactId>avatica</artifactId>
  </dependency>
  <!-- Non-shaded artifact -->
  <dependency>
    <groupId>org.apache.calcite.avatica</groupId>
    <artifactId>avatica-core</artifactId>
  </dependency>
</dependencies>
{% endhighlight %}

In keeping with the convention of previous releases, `org.apache.calcite.avatica:avatica` is a JAR
which contains all of the necessary dependencies of the Avatica client code base. Those classes which
can be safely relocated are done so to reduce the potential for classpath issues.

Avatica 1.9.0 will introduce a new artifact `org.apache.calcite.avatica:avatica-core` which is only
the Avatica client classes without any bundled dependencies. This artifact enables users to build a
classpath with different versions of JARs than what Avatica presently depends upon. This is a "your-mileage-may-vary"
or "void-your-warranty" type of decision (as you are using Avatica with dependecies which we have not tested);
however, some downstream projects do provide reasonable assurances of compatibilities across releases.

## Building your own Avatica client artifact

In some cases, it may be beneficial to provide specific versions of Avatica dependencies. Here is
a brief `pom.xml` which outlines how this can be done.

{% highlight xml %}
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>myorg.custom.client</groupId>
  <artifactId>my-special-app-client</artifactId>
  <packaging>jar</packaging>
  <name>Special Application Client Artifact</name>
  <description>A custom artifact which uses Apache Calcite Avatica for my Org's Special Application</description>

  <properties>
    <myorg.prefix>myorg.custom.client</myorg.prefix>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.calcite.avatica</groupId>
      <artifactId>avatica-core</artifactId>
      <version>1.9.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.httpcomponents</groupId>
      <artifactId>httpclient</artifactId>
      <!-- Override the version from avatica-core (4.5.2) to address a hypothetical bug in httpclient -->
      <version>4.5.3</version>
    </dependency>
    <!-- Include Guava for the "Special Application" -->
    <dependency>
      <groupId>com.google.guava</groupId>
      <artifactId>guava</artifactId>
      <version>17.0</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <!-- Relocate Jackson, Protobuf, Apache Commons HttpClient and HttpComponents, but not Guava.
                   The hypothetical "Special App" would be expecting Guava in the standard location -->
              <relocations>
                <relocation>
                  <pattern>com.fasterxml.jackson</pattern>
                  <shadedPattern>${myorg.prefix}.com.fasterxml.jackson</shadedPattern>
                </relocation>
                <relocation>
                  <pattern>com.google.protobuf</pattern>
                  <shadedPattern>${myorg.prefix}.com.google.protobuf</shadedPattern>
                </relocation>
                <relocation>
                  <pattern>org.apache.http</pattern>
                  <shadedPattern>${myorg.prefix}.org.apache.http</shadedPattern>
                </relocation>
                <relocation>
                  <pattern>org.apache.commons</pattern>
                  <shadedPattern>${myorg.prefix}.org.apache.commons</shadedPattern>
                </relocation>
              </relocations>
              <createDependencyReducedPom>false</createDependencyReducedPom>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
{% endhighlight %}
