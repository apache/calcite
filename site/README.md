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

This directory contains the sources/templates for generating the Apache Calcite website,
[calcite.apache.org](https://calcite.apache.org/). The actual generated content of the website
is present in the [calcite-site](https://github.com/apache/calcite-site) repository.

# Previewing the website locally

## Manually

### Setup your environment

Site generation currently works best with ruby-2.7.4.

1. `cd site`
2. `git clone https://gitbox.apache.org/repos/asf/calcite-site.git target`
3. `sudo apt-get install rubygems ruby2.7-dev zlib1g-dev` (linux)
   `Use RubyInstaller to install rubygems as recommended at https://www.ruby-lang.org/en/downloads/` (Windows)
4. `sudo gem install bundler`
   `gem install bundler` (Windows)
5. `bundle install`

### Add javadoc

1. `cd ..`
2. `./gradlew javadocAggregate`
3. `rm -rf site/target/javadocAggregate`
   `rmdir site\target\javadocAggregate /S /Q` (Windows)
4. `mkdir site/target`
   `mkdir site\target` (Windows)
5. `mv build/docs/javadocAggregate site/target`
   `for /d %a in (build\docs\javadocAggregate*) do move %a site\target` (Windows)

### Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000](http://localhost:4000)

## Using docker

### Setup your environment

1. Install [docker](https://docs.docker.com/install/)
2. Install [docker compose v2](https://docs.docker.com/compose/cli-command/#installing-compose-v2)

### Build site

1. `cd site`
2. `docker compose run build-site`

### Generate javadoc

1. `cd site`
2. `docker compose run generate-javadoc`

### Running development mode locally

You can preview your work while working on the site.

1. `cd site`
2. `docker compose run --service-ports dev`

The web server will be started on [http://localhost:4000](http://localhost:4000)

As you make changes to the site, the site will automatically rebuild.

# Publishing the website

We want to deploy project changes (for example, new committers, PMC members or upcoming talks)
immediately, but we want to deploy documentation of project features only when that feature appears
in a release.

Calcite publishes the website automatically since [CALCITE-3129](https://issues.apache.org/jira/browse/CALCITE-3129),
you do not need to do anything but just merge your changes to the `main` branch,
Github workflows will identify changes to website and automatically cherry-pick it to the `site` branch,
compile and publish it to [calcite-site](https://github.com/apache/calcite-site) repo.

## Non-release publishing

We'll publish the website changes such as community member changes and new blogs immediately after merging.
The rules and scripts are in `.github/workflows/publish-non-release-website-updates.yml`.

## Release publishing

We identify release publishing by checking new release tags. If you are the Release Manager,
you only need to push the new tag 'calcite-x.y.z' to [Calcite Github repo](https://github.com/apache/calcite),
and the Github workflow will do all the rest.
The rules and scripts are in `.github/workflows/publish-website-on-release.yml`.
