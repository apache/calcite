/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const GATE = 'PR merge gate';
const WORKFLOW = '.github/workflows/pr-ci.yml';
const PREVIEW_WORKFLOW = '.github/workflows/pr-ci-preview.yml';
const JENKINS = 'continuous-integration/jenkins/pr-head';
const ACTIONS_APP = 15368;
const PASSING = new Set(['success', 'skipped', 'neutral']);
const GUIDE = 'https://calcite.apache.org/develop/#contributing';

function ownCheck(check) {
  return check.app?.id === ACTIONS_APP && (
    check.name === GATE ||
    check.name === 'Find PRs for merge gate' ||
    /^Update PR merge gate(?: \(\d+\))?$/.test(check.name)
  );
}

function ownWorkflow(run) {
  return run.path?.split('@')[0] === WORKFLOW;
}

function latestBy(items, key) {
  const latest = new Map();
  for (const item of items) {
    const previous = latest.get(key(item));
    if (!previous || item.id > previous.id) {
      latest.set(key(item), item);
    }
  }
  return [...latest.values()];
}

function refsFor(pr) {
  // merge_commit_sha can be stale while GitHub is calculating mergeability.
  return [...new Set([
    pr.head.sha,
    ...(pr.mergeable === true && pr.merge_commit_sha ? [pr.merge_commit_sha] : []),
  ])];
}

async function readPull(github, repo, number) {
  let pr;
  for (let attempt = 0; attempt < 3; attempt++) {
    ({ data: pr } = await github.rest.pulls.get({ ...repo, pull_number: number }));
    if (pr.state !== 'open' || pr.mergeable !== null) {
      break;
    }
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  return pr;
}

async function collect(github, repo, pr, preview = false) {
  const failed = [];
  const pending = [];
  let checked = 0;
  if (pr.mergeable === null) {
    pending.push('GitHub is preparing the current test merge commit');
  }
  for (const ref of refsFor(pr)) {
    const [checks, statuses] = await Promise.all([
      github.paginate(github.rest.checks.listForRef, {
        ...repo, ref, filter: 'latest', per_page: 100,
      }),
      github.paginate(github.rest.repos.listCommitStatusesForRef, {
        ...repo, ref, per_page: 100,
      }),
    ]);
    for (const check of latestBy(checks, item => `${item.app?.id}:${item.name}`)) {
      if (ownCheck(check) || check.name === JENKINS ||
          (preview && check.app?.id === ACTIONS_APP && check.name === 'Preview PR merge gate')) {
        continue;
      }
      checked++;
      const label = `${check.name} (${ref.slice(0, 7)})`;
      if (check.status !== 'completed') {
        pending.push(label);
      } else if (!PASSING.has(check.conclusion)) {
        failed.push(`${label}: ${check.conclusion || 'unknown conclusion'}`);
      }
    }
    // Status history includes superseded failures; only the latest value counts.
    for (const status of latestBy(statuses, item => item.context)) {
      if (status.context === JENKINS || status.context === GATE) {
        continue;
      }
      checked++;
      const label = `${status.context} (${ref.slice(0, 7)})`;
      if (status.state === 'pending') {
        pending.push(label);
      } else if (status.state !== 'success') {
        failed.push(`${label}: ${status.state}`);
      }
    }
  }

  // A queued workflow might not have created any job checks yet. Only runs that
  // actually exist are considered; absent/path-filtered workflows are not required.
  const runs = await github.paginate(github.rest.actions.listWorkflowRunsForRepo, {
    ...repo, head_sha: pr.head.sha, per_page: 100,
  });
  const relevant = runs.filter(run => !ownWorkflow(run) &&
    !(preview && run.path?.split('@')[0] === PREVIEW_WORKFLOW) && (
    run.pull_requests?.some(pull => pull.number === pr.number) ||
    (!run.pull_requests?.length && run.head_branch === pr.head.ref &&
      run.head_repository?.id === pr.head.repo?.id)
  ));
  for (const run of latestBy(relevant, item => `${item.workflow_id}:${item.event}`)) {
    if (run.status !== 'completed') {
      pending.push(`${run.name}: ${run.status}`);
    } else if (!PASSING.has(run.conclusion)) {
      // This also catches workflows that fail before creating jobs.
      failed.push(`${run.name}: ${run.conclusion || 'unknown conclusion'}`);
    }
  }
  return { failed, pending, checked };
}

function describe(result) {
  if (result.failed.length) {
    return {
      conclusion: 'failure',
      title: 'CI checks failed',
      summary: result.failed.map(item => `- ${item}`).join('\n') + `\n\nSee ${GUIDE}.`,
    };
  }
  if (result.pending.length) {
    return {
      title: 'Waiting for running CI checks',
      summary: result.pending.map(item => `- ${item}`).join('\n'),
    };
  }
  return {
    conclusion: 'success',
    title: 'Reported CI checks passed or were skipped',
    summary: `Checked ${result.checked} reported check/status results. ` +
      `Untriggered workflows and ${JENKINS} are excluded.`,
  };
}

function snapshot(pr) {
  return JSON.stringify([
    pr.state, pr.head.sha, pr.base.sha, pr.title, pr.merge_commit_sha,
    pr.labels.map(label => label.name).sort(),
  ]);
}

exports.findPullRequests = async ({ github, context }) => {
  const payload = context.payload;
  if (payload.pull_request) {
    return payload.pull_request.state === 'open' ? [payload.pull_request.number] : [];
  }
  if (ownWorkflow(payload.workflow_run || {}) ||
      (payload.check_run && ownCheck(payload.check_run)) || payload.context === GATE) {
    return [];
  }
  if (context.eventName === 'workflow_dispatch' && payload.inputs?.pull_number) {
    const number = Number(payload.inputs.pull_number);
    if (!Number.isSafeInteger(number) || number < 1) {
      throw new Error('pull_number must be a positive integer');
    }
    return [number];
  }
  const sha = payload.workflow_run?.head_sha || payload.check_run?.head_sha || payload.sha;
  if (!sha && context.eventName !== 'workflow_dispatch') {
    return [];
  }
  // Fork workflow_run payloads may omit pull_requests. Match current heads and
  // merge SHAs explicitly; a completed main push or obsolete head matches no PR.
  const pulls = await github.paginate(github.rest.pulls.list, {
    ...context.repo, state: 'open', per_page: 100,
  });
  return pulls.filter(pr => !sha || pr.head.sha === sha || pr.merge_commit_sha === sha)
    .map(pr => pr.number);
};

exports.preview = async ({ github, context, core }) => {
  const pr = await readPull(github, context.repo, context.issue.number);
  if (pr.state !== 'open') {
    return;
  }
  const result = describe(await collect(github, context.repo, pr, true));
  await core.summary.addHeading(result.title).addRaw(result.summary).write();
  // Informational snapshot: the installed publisher tracks subsequent changes.
  // Do not leave a second failed check behind after the underlying CI is fixed.
  core.info(result.title);
};

exports.publish = async ({ github, context, core, number }, attempt = 0) => {
  const pr = await readPull(github, context.repo, number);
  if (pr.state !== 'open') {
    core.info(`PR #${number} is closed; no merge gate to update.`);
    return;
  }
  const refs = refsFor(pr);
  const update = (sha, state, description) => github.rest.repos.createCommitStatus({
    ...context.repo,
    sha,
    context: GATE,
    state,
    description: description.slice(0, 140),
    target_url: `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
  });
  try {
    // Publish to both SHAs because GitHub may use checks on the test merge SHA.
    // Commit statuses update one context instead of leaving old pending check runs.
    for (const sha of refs) {
      await update(sha, 'pending', 'Refreshing PR CI results');
    }
    const result = describe(await collect(github, context.repo, pr));
    const { data: latest } = await github.rest.pulls.get({
      ...context.repo, pull_number: number,
    });
    if (snapshot(latest) !== snapshot(pr)) {
      if (attempt < 2) {
        // A base-branch update need not emit a PR event. Refresh here instead of
        // leaving the gate pending in anticipation of an event that may never arrive.
        core.info('PR changed during collection; collecting its current state again.');
        return await exports.publish({ github, context, core, number }, attempt + 1);
      }
      throw new Error('PR kept changing during collection');
    }
    for (const sha of refs) {
      await update(sha, result.conclusion || 'pending', result.title);
    }
    await core.summary.addHeading(`PR #${number}: ${result.title}`).addRaw(result.summary).write();
    core.info(`PR #${number}: ${result.title}`);
  } catch (error) {
    // API errors must not turn an unknown CI state into a successful gate.
    for (const sha of refs) {
      await update(sha, 'failure', 'Unable to read PR CI results. Rerun PR CI.');
    }
    throw error;
  }
};
