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
