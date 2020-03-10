#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# HOW TO RELEASE the Looker fork of Calcite:
#
# 1. Start working off the current fork branch: https://github.com/looker-open-source/calcite/tree/looker
# 2. Do whatever needs to be done (rebase on trunk, cherry-pick, remove fixups, etc.).
# 3. Once the code looks good and your Git working directory is clean, run this script, which will:
#    a) Pull all Looker Calcite version tags from GitHub and show you what the latest one is.
#       This is just for convenience.
#    b) Ask you what the next version number should be.
#       Generally we just increment the patch number.
#    c) Reset your local `looker` branch to the current HEAD.
#    d) Update the version line in `gradle.properties` and create a version bump commit.
#    e) Create a release tag pointing to the new version bump commit.
#    f) Provide you with the commands to push your new branch and tag to GitHub,
#       and publish release artifacts to Nexus.
#       This script only automates local changes by design; double-check everything before pushing.

echo "Fetching all tags from looker-open-source repo..." >&2
git fetch git@github.com:looker-open-source/calcite.git --tags && (

  echo -e "\nLatest Looker release tag was '$(git tag --list | grep -E '^calcite-[0-9]+(\.[0-9]+)*-looker$' | sort --version-sort --reverse | head --lines=1)'" >&2
  echo "What should the next version be called?" >&2
  read -p "Input just the numbers and dots (do not include 'calcite-' or '-looker'): " NEXT_NUMBER
  export NEXT_VERSION="${NEXT_NUMBER}-looker"
  export NEXT_TAG="calcite-${NEXT_VERSION}"

  echo -e "\nSetting version number in gradle.properties to '$NEXT_VERSION'." >&2
  sed -i "/^calcite\\.version=.*/c\\calcite.version=$NEXT_VERSION" gradle.properties

  echo -e "\nBuilding '$NEXT_VERSION'..." >&2
  ./gradlew build -x :redis:test && (

    echo -e "\nTests passed! Setting local looker branch." >&2
    git branch -f looker

    export COMMIT_MSG="Prepare for $NEXT_TAG release"
    echo -e "\nTests passed! Creating commit '$COMMIT_MSG'." >&2
    git add gradle.properties && git commit -m "$COMMIT_MSG" && (

      echo -e "\nCreating new tag '$NEXT_TAG'." >&2
      git tag -f "$NEXT_TAG"

      echo -e "\nTake a look around.\nIf everything looks good, you can publish to Nexus with this command:\n" >&2
      echo -e "    ./gradlew -Prelease -PskipSign publishAllPublicationsToLookerNexusRepository\n" >&2
      echo -e "And you can force-push the looker branch and release tag to looker-open-source with this command:\n" >&2
      echo -e "    git push -f git@github.com:looker-open-source/calcite.git looker $NEXT_TAG"
    )
  )
)
