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

# Apache Calcite Avatica site

This directory contains the code for the
[Avatica web site](https://calcite.apache.org/avatica),
a sub-directory of the
[Apache Calcite web site](https://calcite.apache.org).

## Setup

Similar to the instructions to
[set up the Calcite web site](../site/README.md).

1. `cd site`
2. `svn co https://svn.apache.org/repos/asf/calcite/site/avatica target/avatica`
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler github-pages jekyll jekyll-oembed`
5. `bundle install`

## Add javadoc

1. `cd avatica`
2. `mvn -DskipTests site`
3. `rm -rf site/target/avatica/apidocs site/target/avatica/testapidocs`
4. `mv target/site/apidocs target/site/testapidocs site/target/avatica`

## Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000/avatica](http://localhost:4000/avatica)

## Pushing to site

Push the Calcite site, which includes `avatica` as a sub-directory,
as described in its
[README](../site/README.md).
