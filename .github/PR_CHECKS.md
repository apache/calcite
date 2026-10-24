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

- `Check PR title` requires `[CALCITE-<digits>] <Jira summary>` to match the
  complete PR title and at least one commit subject exactly. Other prefixes
  skip this check. Title edits and pushes rerun it; Jira-only edits need a rerun.
- `Check single commit` runs only with the `approve` label. It verifies that
  the label was applied by someone with write, maintain, or admin access,
  then requires exactly one commit. GitHub Approve reviews are separate.
- `PR merge gate` requires all reported checks and statuses on the current
  PR head and test merge commit to pass. Success, skipped, and neutral are
  accepted; running checks remain pending. Untriggered workflows, the gate's
  own jobs, and `continuous-integration/jenkins/pr-head` are excluded.

`pr-ci.yml` finds affected open PRs and refreshes their gate through GitHub
APIs. Updates are serialized per PR and re-read current results, including
queued workflows without job checks. Only the publishing job has
`statuses: write`; no repository code is checked out. There is no separate
script or preview workflow.

The [ruleset in `.asf.yaml`](../.asf.yaml) requires `PR merge gate` on `main`
with no bypass actors. ASF installs it from the default branch; forks need
the equivalent ruleset enabled in repository settings. Jenkins must not be
required by another rule. The publisher's event listeners also need to be
on the default branch; adding the workflow to a PR alone cannot install them.
After installation, manually run **PR CI** on the default branch to refresh
all existing open PRs. The title and commit checks can run in the introducing PR.

Gate updates are asynchronous, so a CI change and its gate update are not
atomic with a merge click. Live validation requires a repository with the
workflows and ruleset installed.
