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

We want to deploy project changes (for example, new committers, PMC members or upcoming talks)
immediately, but we want to deploy documentation of project features only when that feature appears
in a release.

The building and publishing of the website is completely automated using Github actions, so you should simply commit
your changes to main. If you are committing a change to the website that needs to be published immediately, the
Github action uses these [rules](../.github/workflows/publish-non-release-website-updates.yml#L7).

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

Publishing the website is usually simple, you just need to copy the newly generated site content to the [calcite-site](https://github.com/apache/calcite-site) repository.

But sometimes, especially when we upgraded Jekyll version, the `js` and `css` files may be renamed or removed, copying will not remove these stale files in calcite-site.

Hence, a safer way is to remove the old files in calcite-site for the first step, then do the copying.
