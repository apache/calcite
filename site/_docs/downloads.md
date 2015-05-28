---
layout: docs
title: Downloads
permalink: /docs/downloads.html
---

<!--
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
-->

Calcite is released as a source artifact, and also through Maven.

# Source releases

Release          | Date       | Commit   | Notes | Download
:--------------- | :--------- | :------- | :---- | :-------
{% for post in site.categories.release %}{{ post.version }} | {{ post.date | date_to_string }} | <a href="https://github.com/apache/incubator-calcite/commit/{{ post.sha }}">{{ post.sha }}</a> | <a href="history.html#{{ post.tag }}">notes</a> | <a href="http://{% if forloop.index0 < 2 %}www.apache.org/dyn/closer.cgi{% else %}archive.apache.org/dist{% endif %}/incubator/calcite/{% if post.fullVersion %}{{ post.fullVersion }}{% else %}apache-calcite-{{ post.version }}{% endif %}">src</a>
      {% endfor %}

# Maven artifacts

Add the following to the dependencies section of your `pom.xml` file:

{% for post in site.categories.release limit:1 %}
{% assign current_release = post %}
{% endfor %}

{% highlight xml %}
<dependencies>
  <dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-core</artifactId>
    <version>{{ current_release.version }}</version>
  </dependency>
</dependencies>
{% endhighlight %}

Also include `<dependency>` elements for any extension modules you
need: `calcite-mongodb`, `calcite-spark`, `calcite-splunk`, and so
forth.