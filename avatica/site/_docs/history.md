---
layout: docs
title: History
permalink: "/docs/history.html"
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

For a full list of releases, see
<a href="https://github.com/apache/calcite/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/).

## 1.7.0 / (Under Development)
{: #v1-7-0}

One notable change is that the use of JUL (java.util.logging) has been replaced
with [SLF4J](http://slf4j.org/). SLF4J provides an API which Calcite can use
independent of the logging implementation. This ultimately provides additional
flexibility to users, allowing them to configure Calcite's logging within their
own chosen logging framework. This work was done in [CALCITE-669](https://issues.apache.org/jira/browse/CALCITE-669).

For users experienced with configuring JUL in Calcite previously, there are some
differences as some the JUL logging levels do not exist in SLF4J: `FINE`,
`FINER`, and `FINEST`, specifically. To deal with this, `FINE` was mapped
to SLF4J's `DEBUG` level, while `FINER` and `FINEST` were mapped to SLF4J's `TRACE`.

## Past releases

Please refer to the [Calcite release page](https://calcite.apache.org/docs/history.html)
for information about previous Avatica releases.
