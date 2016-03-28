---
layout: page
title: Community
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

* TOC
{:toc}

# Project Members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}  {{ c.name }} (<a href="http://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="http://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endfor %}

# Mailing Lists

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

# Help

Need help with Calcite? Try these resources:

* **Mailing Lists**.
  The best option is to send email to the developers list
  [dev@calcite.apache.org](mailto:dev@calcite.apache.org). All
  of the historic traffic is available in the
  [archive](http://mail-archives.apache.org/mod_mbox/calcite-dev/). To
  subscribe to the user list, please send email to
  [dev-subscribe@calcite.apache.org](mailto:dev-subscribe@calcite.apache.org).
* **Bug Reports**.
  Please file any issues you encounter or fixes you'd like on the
  [Calcite Jira](https://issues.apache.org/jira/browse/CALCITE). We welcome
  patches!
* **StackOverflow**.
  [StackOverflow](http://stackoverflow.com) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [github](https://github.com/apache/calcite/tree/master/avatica).
