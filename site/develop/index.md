---
layout: page
title: Developing
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

Want to help add a feature or fix a bug?

## Project Members

Name | Apache Id | Github | Organization | Role
:--- | :-------- | :----- | :---         | :---
{% for c in site.data.contributors %}  {{ c.name }} | <a href="http://people.apache.org/committer-index#{{ c.apacheId }}">{{ c.apacheId }}</a> | <a href="http://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endfor %}

## Mailing Lists

There are several development mailing lists for Calcite:

* [dev@calcite.apache.org](mailto:dev@calcite.apache.org) - Development discussions
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/)]
* [issues@calcite.apache.org](mailto:issues@calcite.apache.org) - Bug tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-issues/)]
* [commits@calcite.apache.org](mailto:commits@calcite.apache.org) - Git tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-commits/)]

You can subscribe to the lists by sending email to
*list*-subscribe@calcite.apache.org and unsubscribe by sending email to
*list*-unsubscribe@calcite.apache.org.

## Source code

Calcite uses git for version control.  The canonical source is in
[Apache](https://git-wip-us.apache.org/repos/asf/calcite.git),
but most people find the
[Github mirror](https://github.com/apache/calcite) more
user-friendly.

## Download source, build, and run tests

Prerequisites are git, maven (3.2.1 or later) and Java (JDK 1.7 or
later, 1.8 preferred) on your path.

Create a local copy of the git repository, cd to its root directory,
then build using maven:

{% highlight bash %}
$ git clone git://github.com/apache/calcite.git
$ cd calcite
$ mvn install
{% endhighlight %}

Please check our [contributing guidelines]({{ site.baseurl }}/docs/howto.html#contributing).

