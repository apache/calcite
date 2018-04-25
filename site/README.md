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

This directory contains the code for the Apache Calcite web site,
[calcite.apache.org](https://calcite.apache.org/).

## Setup

Site generation currently works best with ruby-2.4.1.

1. `cd site`
2. `svn co https://svn.apache.org/repos/asf/calcite/site target`
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler github-pages jekyll jekyll-oembed`
5. `bundle install`

## Add javadoc

1. `cd ..`
2. `mvn -DskipTests site`
3. `rm -rf site/target/apidocs site/target/testapidocs`
4. `mv target/site/apidocs target/site/testapidocs site/target`

## Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000](http://localhost:4000)

## Pushing to site

1. `cd site`
2. `svn co https://svn.apache.org/repos/asf/calcite/site target`
3. `cd target`
4. `svn status`
5. You'll need to `svn add` any new files
6. `svn ci`

Within a few minutes, svnpubsub should kick in and you'll be able to
see the results at
[calcite.apache.org](https://calcite.apache.org/).

This process also publishes Avatica's web site. Avatica's web site has
separate source (under `avatica/site`) but configures Jekyll to
generate files to `site/target/avatica`, which becomes an
[avatica](http://calcite.apache.org/avatica)
sub-directory when deployed. See
[Avatica site README](../avatica/site/README.md).

## Site branch

We want to deploy project changes (for example, new committers, PMC
members or upcoming talks) immediately, but we want to deploy
documentation of project features only when that feature appears in a
release. For this reason, we generally edit the site on the "site" git
branch.

Before making a release, release manager must ensure that "site" is in
sync with "master". Immediately after a release, the release manager
will publish the site, including all of the features that have just
been released. When making an edit to the site, a Calcite committer
must commit the change to the git "master" branch (as well as
subversion, to publish the site, of course). If the edit is to appear
on the site immediately, the committer should then cherry-pick the
change into the "site" branch.  If there have been no feature-related
changes on the site since the release, then "site" should be a
fast-forward merge of "master".