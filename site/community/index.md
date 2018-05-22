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

* 2018/06/12 [SIGMOD/PODS 2018](https://sigmod2018.org/index.shtml) (Houston, TX)
  * [Apache Calcite: A Foundational Framework for Optimized Query
    Processing Over Heterogeneous Data
    Sources](https://sigmod2018.org/sigmod_industrial_list.shtml) (Edmon
    Begoli, Jesús Camacho-Rodríguez, Julian Hyde, Michael Mior, Daniel
    Lemire)
* 2018/06/27 [SF Big Analytics streaming meetup, hosted by Lyft](https://www.meetup.com/SF-Big-Analytics/)
  (San Francisco, CA)
  * Foundations of streaming SQL or: How I learned to love stream and
    table theory (Tyler Akidau)
  * Data all over the place! How Apache Calcite brings SQL and sanity to
    streaming and spatial data (Julian Hyde)
* 2018/09/25 [ApacheCon 2018](https://www.apachecon.com/acna18/) (Montréal, Canada)
  * Spatial query on vanilla databases (Julian Hyde)
  * Don't optimize my queries, optimize my data! (Julian Hyde)

# Project Members

Name (Apache ID) | Github | Org | Role
:--------------- | :----- | :-- | :---
{% for c in site.data.contributors %}{% if c.homepage %}<a href="{{ c.homepage }}">{{ c.name }}</a>{% else %}{{ c.name }}{% endif %} (<a href="http://people.apache.org/phonebook.html?uid={{ c.apacheId }}">{{ c.apacheId }}</a>) | <a href="http://github.com/{{ c.githubId }}"><img width="64" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}"></a> | {{ c.org }} | {{ c.role }}
{% endfor %}

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
  [archive](http://mail-archives.apache.org/mod_mbox/calcite-dev/). To
  subscribe to the user list, please send email to
  [dev-subscribe@calcite.apache.org](mailto:dev-subscribe@calcite.apache.org).
* **Bug Reports**.
  Please file any issues you encounter or fixes you'd like on the
  [Calcite Jira](https://issues.apache.org/jira/browse/CALCITE). We welcome
  patches and pull-requests!
* **StackOverflow**.
  [StackOverflow](http://stackoverflow.com/questions/tagged/calcite) is a wonderful resource for
  any developer. Take a look over there to see if someone has answered
  your question.
* **Browse the code**.
  One of the advantages of open source software is that you can browse the code.
  The code is available on [github](https://github.com/apache/calcite/tree/master).

# Talks

Want to learn more about Calcite?

Watch some presentations and read through some slide decks about
Calcite, or attend one of the [upcoming talks](#upcoming-talks).

## Apache Calcite: One planner fits all

Voted [Best Lightning Talk at XLDB-2015](http://www.xldb.org/archives/2015/05/best-lightning-talks-selected/);
[[video](https://www.youtube.com/watch?v=5_MyORYjq3w)],
[[slides](http://www.slideshare.net/julianhyde/apache-calcite-one-planner-fits-all)].

{% oembed https://www.youtube.com/watch?v=5_MyORYjq3w %}

## Streaming SQL

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=b7HENkvd1uU)],
[[slides](http://www.slideshare.net/julianhyde/streaming-sql-63554778)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/calcite-streaming-sql-san-jose-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=b7HENkvd1uU %}

## Cost-based Query Optimization in Apache Phoenix using Apache Calcite

At Hadoop Summit, San Jose, CA, 2016
[[video](https://www.youtube.com/watch?v=gz9X7JD8BAU)],
[[slides](http://www.slideshare.net/julianhyde/costbased-query-optimization-in-apache-phoenix-using-apache-calcite)],
[[pdf](https://github.com/julianhyde/share/blob/master/slides/phoenix-on-calcite-hadoop-summit-2016.pdf?raw=true)].

{% oembed https://www.youtube.com/watch?v=gz9X7JD8BAU %}

## Planning with Polyalgebra: Bringing together relational, complex and machine learning algebra

As Hadoop Summit, Dublin, 2016
[[video](https://www.youtube.com/watch?v=fHZqbe3iPMc)],
[[slides](http://www.slideshare.net/julianhyde/planning-with-polyalgebra-bringing-together-relational-complex-and-machine-learning-algebra)].

{% oembed https://www.youtube.com/watch?v=fHZqbe3iPMc %}

## More talks

* <a href="https://github.com/julianhyde/share/blob/master/slides/calcite-algebra-edw-2015.pdf?raw=true">Why you care about relational algebra (even though you didn't know it)</a> (Washington DC, 2015)
* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-seattle-2014.pdf?raw=true">Cost-based optimization in Hive 0.14</a> (Seattle, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="http://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)
