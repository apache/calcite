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

# Apache Calcite docs site

This directory contains the code for the Apache Calcite (incubating) web site,
[calcite.incubator.apache.org](https://calcite.incubator.apache.org/).

## Setup

1. `cd site`
2. svn co https://svn.apache.org/repos/asf/incubator/calcite/site target
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler github-pages jekyll`
5. `bundle install`

## Add javadoc

1. `cd ..`
2. `mvn -DskipTests site`
3. `mv target/site/apidocs site/target`

## Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000](http://localhost:4000)

## Pushing to site

1. `cd site/target`
2. `svn status`
3. You'll need to `svn add` any new files
4. `svn ci`

Within a few minutes, svnpubsub should kick in and you'll be able to
see the results at
[calcite.incubator.apache.org](https://calcite.incubator.apache.org/).
