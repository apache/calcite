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
#    `git fetch git@github.com:looker-open-source/calcite.git looker && git checkout FETCH_HEAD`
# 2. Do whatever needs to be done (rebase on trunk, cherry-pick, remove fixups, etc.),
#    but don't bother updating the `calcite.version` line in `gradle.properties`.
#    There should only be "business logic" commits,
#    and every commit that is not yet upstreamed (called "fix-up" commits)
#    should reference a Calcite Jira ticket in its message,
#    and have a PR open to upstream it to Apache's main repo, if possible.
# 3. Once the code looks good and your Git working directory is clean, run this script
#    which will do all of this (mostly) automatically:
#    a) Pull all Looker Calcite version tags from GitHub and show you what the latest one is.
#       This is just for convenience.
#    b) Ask you what the next version number should be.
#       The major number should always be whatever's in Apache's `main` branch.
#       The minor number should probably be 1 less than whatever's in Apache's `main` branch.
#       Increment the patch number when you make backward compatible bug fixes.
#       Generally, we just increment the patch number.
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
  # MacOS uses BSD sed, which works differently from GNU sed on Linux.
  if [[ "$(uname -s)" == "Darwin" ]]
  then
    # https://stackoverflow.com/questions/5694228/sed-in-place-flag-that-works-both-on-mac-bsd-and-linux
    # https://stackoverflow.com/questions/64373364/how-to-fix-sed-command-on-macos-with-error-extra-characters-after-at-the-end-o
    sed -i.bak "/^calcite\\.version=.*/c\\
calcite.version=$NEXT_VERSION
" gradle.properties && rm gradle.properties.bak
  else
    sed -i "/^calcite\\.version=.*/c\\calcite.version=$NEXT_VERSION" gradle.properties
  fi

  # $(exit $?) has the same status code as the previous `sed` command.
  $(exit $?) && (

    echo -e "\nBuilding '$NEXT_VERSION'..." >&2
    ./gradlew build -x :redis:test && (

      export LOOKER_COMMIT="$(git rev-parse --short HEAD)"

      export COMMIT_MSG="Prepare for $NEXT_TAG release"
      echo -e "\nTests passed! Creating commit '$COMMIT_MSG'." >&2
      git add gradle.properties && git commit -m "$COMMIT_MSG" && (

        echo -e "\nCreating new tag '$NEXT_TAG'." >&2
        git tag -f "$NEXT_TAG"

        echo -e "\nTake a look around." >&2
        echo -e "You should see (starting from the most recent tip of your history):" >&2
        echo -e "- A tagged release commit, which just updates the version and nothing else." >&2
        echo -e "  There should be only 1 such commit in the whole history." >&2
        echo -e "- The latest fixup commit (business logic that is not upstreamed)." >&2
        echo -e "  This becomes the new tip of the looker branch." >&2
        echo -e "- Prior fixups, if any..." >&2
        echo -e "- All commits from upstream..." >&2
        echo -e "\nIf everything looks good, you can publish to Artifact Registry with this command:\n" >&2
        echo -e "    ./looker-upload-artifact-registry.sh\n" >&2
        echo -e "And you can push the release tag and force-push the looker branch to looker-open-source with these commands:\n" >&2
        echo -e "    git push git@github.com:looker-open-source/calcite.git $NEXT_TAG"
        echo -e "    git push -f git@github.com:looker-open-source/calcite.git $LOOKER_COMMIT:looker"
      )
    )
  )
)
