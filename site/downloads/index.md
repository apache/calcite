---
layout: page
title: Downloads
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

Calcite is released as a source artifact, and also through Maven.

# Source releases

Release          | Date       | Commit   | Download
:--------------- | :--------- | :------- | :-------
{% for post in site.categories.release %}{% comment %}
{% endcomment %}{% if post.fullVersion %}{% comment %}
{% endcomment %}{% assign v = post.fullVersion %}{% comment %}
{% endcomment %}{% else %}{% comment %}
{% endcomment %}{% capture v %}apache-calcite-{{ post.version }}{% endcapture %}{% comment %}
{% endcomment %}{% endif %}{% comment %}
{% endcomment %}{% if forloop.index0 < 1 %}{% comment %}
{% endcomment %}{% capture p %}https://www.apache.org/dyn/closer.lua?filename=calcite/{{ v }}{% endcapture %}{% comment %}
{% endcomment %}{% assign q = "&action=download" %}{% comment %}
{% endcomment %}{% assign d = "https://www.apache.org/dist" %}{% comment %}
{% endcomment %}{% elsif forloop.rindex < 8 %}{% comment %}
{% endcomment %}{% capture p %}https://archive.apache.org/dist/incubator/calcite/{{ v }}{% endcapture %}{% comment %}
{% endcomment %}{% assign q = "" %}{% comment %}
{% endcomment %}{% assign d = "https://archive.apache.org/dist/incubator" %}{% comment %}
{% endcomment %}{% else %}{% comment %}
{% endcomment %}{% capture p %}https://archive.apache.org/dist/calcite/{{ v }}{% endcapture %}{% comment %}
{% endcomment %}{% assign q = "" %}{% comment %}
{% endcomment %}{% assign d = "https://archive.apache.org/dist" %}{% comment %}
{% endcomment %}{% endif %}{% comment %}
{% endcomment %}{% capture d1 %}{{ post.date | date: "%F"}}{% endcapture %}{% comment %}
{% endcomment %}{% capture d2 %}2017-08-31{% endcapture %}{% comment %}
{% endcomment %}{% capture d3 %}2018-06-01{% endcapture %}{% comment %}
{% endcomment %}{% if d1 > d2 %}{% comment %}
{% endcomment %}{% assign digest = "sha256" %}{% comment %}
{% endcomment %}{% else %}{% comment %}
{% endcomment %}{% assign digest = "md5" %}{% comment %}
{% endcomment %}{% endif %}{% comment %}
{% endcomment %}<a href="{{ site.baseurl }}/docs/history.html#{{ post.tag }}">{{ post.version }}</a>{% comment %}
{% endcomment %} | {{ post.date | date_to_string }}{% comment %}
{% endcomment %} | <a href="https://github.com/apache/calcite/commit/{{ post.sha }}">{{ post.sha | slice: 0, 7 }}</a>{% comment %}
{% endcomment %} | <a href="{{ p }}/{{ v }}-src.tar.gz{{ q }}">tar</a>{% comment %}
{% endcomment %} (<a href="{{ d }}/calcite/{{ v }}/{{ v }}-src.tar.gz.{{ digest }}">digest</a>{% comment %}
{% endcomment %} <a href="{{ d }}/calcite/{{ v }}/{{ v }}-src.tar.gz.asc">pgp</a>){% comment %}
{% endcomment %}{% if d1 < d3 %}{% comment %}
{% endcomment %} {% raw %}<br>{% endraw %}{% comment %}
{% endcomment %} <a href="{{ p }}/{{ v }}-src.zip{{ q }}">zip</a>{% comment %}
{% endcomment %} (<a href="{{ d }}/calcite/{{ v }}/{{ v }}-src.zip.{{ digest }}">digest</a>{% comment %}
{% endcomment %} <a href="{{ d }}/calcite/{{ v }}/{{ v }}-src.zip.asc">pgp</a>){% comment %}
{% endcomment %}{% endif %}{% comment %}
{% endcomment %}
{% endfor %}

To download a source distribution for a particular release, click on
the *tar* link (for older releases, *zip* format is also available).

The commit hash links to github, which contains the release's version
control history but does not contain the definitive source artifacts.

For fast downloads, current source distributions are hosted on mirror servers;
older source distributions are in the
[archive](https://archive.apache.org/dist/calcite/)
or [incubator archive](https://archive.apache.org/dist/incubator/calcite/).
If a download from a mirror fails, retry, and the second download will likely
succeed.

For security, hash and signature files are always hosted at
[Apache](https://www.apache.org/dist).

# Verify the integrity of the files

You must verify the integrity of the downloaded file using the PGP
signature (.asc file) or a hash (.sha256; .md5 for older releases).
For more information why this must be done, please read
[Verifying Apache Software Foundation Releases](https://www.apache.org/info/verification.html).

To verify the signature using GPG or PGP, please do the following:

1. Download the release artifact and the corresponding PGP signature from the table above.
2. Download the [Apache Calcite KEYS](https://www.apache.org/dist/calcite/KEYS) file.
3. Import the KEYS file and verify the downloaded artifact using one of the following methods:
{% highlight shell %}
% gpg --import KEYS
% gpg --verify downloaded_file.asc downloaded_file
{% endhighlight %}

or

{% highlight shell %}
% pgpk -a KEYS
% pgpv downloaded_file.asc
{% endhighlight %}

or

{% highlight shell %}
% pgp -ka KEYS
% pgp downloaded_file.asc
{% endhighlight %}

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
