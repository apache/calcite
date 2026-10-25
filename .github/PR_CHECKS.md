<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PR checks

The [ruleset in `.asf.yaml`](../.asf.yaml) requires the 15 CI jobs in
`main.yml`, Gradle wrapper `Validation`, `SonarCloud` (code scanning),
`SonarCloud Code Analysis`, and the two PR metadata checks below to merge
into `main`, with no bypass actors. GitHub enforces these checks directly.
`continuous-integration/jenkins/pr-head` is not required by this ruleset.

- `Check PR title` requires `[CALCITE-<digits>] <Jira summary>` to match the
  complete PR title and at least one commit subject exactly. Other prefixes
  skip this check. Title edits and pushes rerun it; Jira-only edits need a rerun.
- `Check single commit` runs only with the `LGTM-will-merge-soon` label. It verifies that
  the label was applied by someone with write, maintain, or admin access,
  then requires exactly one commit. GitHub Approve reviews are separate.

Existing CI triggers and jobs are unchanged. If a required workflow does
not run (for example, `main.yml` ignores PRs changing only `site/**`), its
missing checks block merging. GitHub accepts success, skipped, and neutral
check conclusions; pending, failed, or cancelled checks block merging.
Slow tests and website builds are not added to the required list.

When adding or renaming a required check, update its context in `.asf.yaml`.

ASF installs the ruleset from the default branch. Forks need the equivalent
ruleset enabled in repository settings. Replace any old `PR merge gate`
requirement when deploying this version, and ensure no other rule requires
Jenkins. Live merge enforcement requires the ruleset to be installed on
GitHub.
