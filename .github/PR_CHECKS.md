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

`Check PR title` compares `[CALCITE-<digits>] <Jira summary>` with the complete
PR title and the first line of at least one commit. Case, spacing, and
punctuation must match; commit bodies may differ. Issue numbers have no fixed
length. Other prefixes skip this consistency check. Editing the PR title or
pushing commits triggers a new check; Jira-only changes require a manual rerun.

`Check single commit` runs only while the PR has the exact label `approve`.
It verifies the account which applied that label, including when rerunning
on a subsequent push. Write, maintain, or admin permission on the base
repository authorizes the check; this includes inherited committer/PMC team
access. Only then is the commit count required to be one. Without the label,
the job is skipped. This label is separate from GitHub's **Approve review**.

`PR merge gate` aggregates the latest reported checks and commit statuses on
the current PR head and test merge commit. Failures, cancellations, and errors
block merging; running or queued checks keep the gate pending. Success,
skipped, and neutral conclusions are accepted. Workflows which have not been
triggered do not participate. The exact context
`continuous-integration/jenkins/pr-head` is excluded, whether reported as a
check or as a commit status. The aggregator excludes its own control jobs to
avoid waiting for itself.

The publisher reacts to PR changes, workflow starts/completions, external
check events, and commit status events. It reads current API state instead of
trusting an older event's conclusion. Updates are serialized per PR, and each
update replaces the previous gate with a pending status before collecting
results. Old PR heads and closed PRs are ignored. New CI jobs do not need to
be added to a required-check list. Existing CI triggers and path filters are
unchanged.

Updates follow asynchronous GitHub events. There is a propagation delay between
a CI state change and the corresponding gate update; this workflow cannot make
that update atomic with a merge click. GitHub's native required checks are an
explicit list, so a dynamic "all reported checks except Jenkins" policy cannot
provide that stronger guarantee through repository workflows alone.

The [ruleset in `.asf.yaml`](../.asf.yaml) requires only `PR merge gate`, from
the GitHub Actions app, on `main`, with no bypass actors. Repository rules
must actually be installed for the merge button to enforce the gate. ASF
applies `.asf.yaml` from the default branch; personal forks need the equivalent
ruleset installed through Settings or the GitHub API. Other rulesets also
apply, so Jenkins must not remain required by another rule.

The publisher needs `statuses: write` to report the gate and always loads its
script from the repository's default branch. It never executes PR code or
loads PR artifacts/caches with that token. The title and count checks use
read-only API access and can run in the PR introducing these workflows.
Neither runs on pushes to `main` or on PR closure. CI events after a main push
are safely ignored when they match no open PR.

`Preview PR merge gate` runs the same aggregation code with a read-only token
from the PR checkout, so the introducing PR can show actual CI results before
the publisher is installed. Its Actions summary is informational: a pending
or failed underlying check is displayed there without leaving an extra stale
failure after that underlying check is fixed. The publisher tracks subsequent
results automatically once installed.

For a complete live test before upstream merge, install these files on your
fork's default branch and enable its Actions and ruleset, then open a PR in
that fork. GitHub requires `workflow_run`, `check_run`, and `status` listeners
to exist on the default branch. Merely adding them in an upstream PR cannot
install these listeners or change the upstream merge button. To refresh a
specific PR manually, use the **PR CI** workflow's `pull_number` input; leaving
it empty refreshes all open PRs. Run this once for pre-existing PRs when first
installing the ruleset.

No unit-test workflow or mocked test suite is included. Live PR validation is
performed by the contributor.

References: [GitHub workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows),
[workflow name filters](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onworkflow_runworkflows),
and [ASF rulesets](https://github.com/apache/infrastructure-asfyaml#rulesets).
