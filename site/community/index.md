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

# Upcoming talks

## An introduction to query processing & Apache Calcite

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021.

## Event timestamp semantic based streaming SQL

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021.

## Implementing spatial queries using algebra rewrites

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021.

## Apache Calcite integration into Hazelcast distributed SQL engine

At [Calcite Virtual Meetup](https://www.meetup.com/Apache-Calcite/events/275461117/), January 20, 2021.

# Project Members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}{% unless c.emeritus %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="https://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="https://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}https://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endunless %}{% endfor %}

Emeritus members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}{% if c.emeritus %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="https://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="https://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}https://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endif %}{% endfor %}

# Mailing Lists

There are several mailing lists for Calcite:

* [dev@calcite.apache.org](mailto:dev@calcite.apache.org) &mdash; Development discussions
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/)]
* [issues@calcite.apache.org](mailto:issues@calcite.apache.org) &mdash; Bug tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-issues/)]
* [commits@calcite.apache.org](mailto:commits@calcite.apache.org) &mdash; Git tracking
  [[archive](https://mail-archives.apache.org/mod_mbox/calcite-commits/)]

You can subscribe to the lists by sending email to
*{list}*-subscribe@calcite.apache.org and unsubscribe by sending email to
*{list}*-unsubscribe@calcite.apache.org (where *{list}* is either "dev", "issues", or "commits").

# Help

Need help with Calcite? Try these resources:

* **Mailing Lists**.
  The best option is to send email to the developers list
  [dev@calcite.apache.org](mailto:dev@calcite.apache.org). All
  of the historic traffic is available in the
  [archive](https://mail-archives.apache.org/mod_mbox/calcite-dev/). To
  subscribe to the user list, please send email to
  [dev-subscribe@calcite.apache.org](mailto:dev-subscribe@calcite.apache.org).
* **Bug Reports**.
  Please file any issues you encounter or fixes you'd like on the
  [Calcite Jira](https://issues.apache.org/jira/browse/CALCITE). We welcome
  patches and pull-requests!
* **StackOverflow**.
  [StackOverflow](https://stackoverflow.com/questions/tagged/calcite) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [github](https://github.com/apache/calcite/tree/master).

# Talks

Want to learn more about Calcite?

Watch some presentations and read through some slide decks about
Calcite, or attend one of the [upcoming talks](#upcoming-talks).

## Fast federated SQL with Apache Calcite

At [ApacheCon Europe 2019](https://aceu19.apachecon.com/), Berlin, Germany, October 24, 2019;
[[summary](https://aceu19.apachecon.com/session/fast-federated-sql-apache-calcite)],
[[video](https://youtu.be/4JAOkLKrcYE)].


## One SQL to Rule Them All - an Efficient and Syntactically Idiomatic Approach to Management of Streams and Tables

At [SIGMOD/PODS 2019](https://sigmod2019.org/sigmod_industry_list), Amsterdam, Netherlands, 2019
and [Beam Summit Europe 2019](https://beam-summit.firebaseapp.com/schedule/);
[[paper](https://arxiv.org/abs/1905.12133)],
[[review](https://blog.acolyer.org/2019/07/03/one-sql-to-rule-them-all/)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/one-sql-to-rule-them-all-beam-summit-2019.pdf?raw=true)],
[[video](https://www.youtube.com/watch?v=9f4igtyNseo)].

## Apache Calcite: A Foundational Framework for Optimized Query Processing Over Heterogeneous Data Sources

At [SIGMOD/PODS 2018](https://sigmod2018.org/index.shtml), Houston, TX, 2018;
[[paper](https://arxiv.org/pdf/1802.10233)],
[[slides](https://www.slideshare.net/julianhyde/apache-calcite-a-foundational-framework-for-optimized-query-processing-over-heterogeneous-data-sources)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/calcite-sigmod-2018.pdf?raw=true)].

## Spatial query on vanilla databases

At ApacheCon North America, 2018;
[[slides](https://www.slideshare.net/julianhyde/spatial-query-on-vanilla-databases)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/calcite-spatial-apache-con-2018.pdf?raw=true).

## Apache Calcite: One planner fits all

Voted [Best Lightning Talk at XLDB-2015](https://www.xldb.org/archives/2015/05/best-lightning-talks-selected/);
[[video](https://www.youtube.com/watch?v=5_MyORYjq3w)],
[[slides](https://www.slideshare.net/julianhyde/apache-calcite-one-planner-fits-all)].

{% oembed https://www.youtube.com/watch?v=5_MyORYjq3w %}

## Streaming SQL

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=b7HENkvd1uU)],
[[slides](https://www.slideshare.net/julianhyde/streaming-sql-63554778)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/calcite-streaming-sql-san-jose-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=b7HENkvd1uU %}

## Cost-based Query Optimization in Apache Phoenix using Apache Calcite

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=gz9X7JD8BAU)],
[[slides](https://www.slideshare.net/julianhyde/costbased-query-optimization-in-apache-phoenix-using-apache-calcite)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/phoenix-on-calcite-hadoop-summit-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=gz9X7JD8BAU %}

## Planning with Polyalgebra: Bringing together relational, complex and machine learning algebra

As Hadoop Summit, Dublin, 2016
[[video](https://www.youtube.com/watch?v=fHZqbe3iPMc)],
[[slides](https://www.slideshare.net/julianhyde/planning-with-polyalgebra-bringing-together-relational-complex-and-machine-learning-algebra)].

{% oembed https://www.youtube.com/watch?v=fHZqbe3iPMc %}

## More talks

* <a href="https://github.com/julianhyde/share/blob/master/slides/calcite-algebra-edw-2015.pdf?raw=true">Why you care about relational algebra (even though you didn't know it)</a> (Washington DC, 2015)
* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-seattle-2014.pdf?raw=true">Cost-based optimization in Hive 0.14</a> (Seattle, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="https://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)
