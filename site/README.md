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

You can build the site manually using your environment or use the docker compose file.

## Manually

### Setup your environment

Site generation currently works best with ruby-2.5.1.

1. `cd site`
2. `git clone https://gitbox.apache.org/repos/asf/calcite-site.git target`
3. `sudo apt-get install rubygems ruby2.5-dev zlib1g-dev` (linux)
   `Use RubyInstaller to install rubygems as recommended at https://www.ruby-lang.org/en/downloads/` (Windows)
4. `sudo gem install bundler`
   `gem install bundler` (Windows)
5. `bundle install`

### Add javadoc

1. `cd ..`
2. `mvn -DskipTests site`
3. `rm -rf site/target/apidocs site/target/testapidocs`
   `rmdir site\target\apidocs site\target\testapidocs /S /Q` (Windows)
4. `mv target/site/apidocs target/site/testapidocs site/target`
   `for /d %a in (target\site\apidocs* target\site\testapidocs*) do move %a site\target` (Windows)

### Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000](http://localhost:4000)

## Using docker

### Setup your environment

1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

### Build site

1. `cd site`
2. `docker-compose run build-site`

### Generate javadoc

1. `cd site`
2. `docker-compose run generate-javadoc`

### Running development mode locally

You can preview your work while working on the site.

1. `cd site`
2. `docker-compose run --service-ports dev`

The web server will be started on [http://localhost:4000](http://localhost:4000)

As you make changes to the site, the site will automatically rebuild.

## Pushing to site

1. `cd site/target`
2. `git init`
3. `git remote add origin https://github.com/apache/calcite-site`
4. `git reset origin/master --soft`

If you have not regenerated the javadoc and they are missing, restore them:

6. `git reset -- apidocs/`
7. `git reset -- testapidocs/`
8. `git checkout -- apidocs/`
9. `git checkout -- testapidocs/`

Restore the avatica site

10. `git reset -- avatica/`
11. `git checkout -- avatica/`

10. `git add .`
11. Commit: `git commit -m "Your commit message goes here"`
12. Push the site: `git push origin master`

Within a few minutes, gitpubsub should kick in and you'll be able to
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
git, to publish the site, of course). If the edit is to appear
on the site immediately, the committer should then cherry-pick the
change into the "site" branch.  If there have been no feature-related
changes on the site since the release, then "site" should be a
fast-forward merge of "master".
