#!/usr/bin/env bash
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

set -euo pipefail

require_command() {
  local command=$1

  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "::error::Required command '${command}' was not found"
    exit 1
  fi
}

request() {
  local method=$1
  local path=$2
  local data=${3:-}
  local response
  local status
  local url

  response=$(mktemp)
  url="${JIRA_BASE_URL%/}/${path#/}"

  # Keep response body separate from the HTTP status so failed Jira calls can
  # surface the actual error message in the GitHub Actions log.
  if [[ -n "${data}" ]]; then
    if ! status=$(curl --silent --show-error --location \
        --request "${method}" \
        "${jira_auth_args[@]}" \
        --header 'Accept: application/json' \
        --header 'Content-Type: application/json' \
        --data "${data}" \
        --write-out '%{http_code}' \
        --output "${response}" \
        "${url}"); then
      cat "${response}" >&2
      rm -f "${response}"
      return 1
    fi
  else
    if ! status=$(curl --silent --show-error --location \
        --request "${method}" \
        "${jira_auth_args[@]}" \
        --header 'Accept: application/json' \
        --write-out '%{http_code}' \
        --output "${response}" \
        "${url}"); then
      cat "${response}" >&2
      rm -f "${response}"
      return 1
    fi
  fi

  if ((status < 200 || status >= 300)); then
    echo "::error::Jira API ${method} ${path} failed with HTTP ${status}" >&2
    cat "${response}" >&2
    rm -f "${response}"
    return 1
  fi

  cat "${response}"
  rm -f "${response}"
}

extract_issue_keys() {
  local text=$1

  # Calcite PR titles are expected to contain CALCITE-XXXX, often in brackets
  # but not necessarily at the beginning. The body is checked later only as a
  # fallback for cosmetic or older PR descriptions.
  printf '%s\n' "${text}" \
    | grep -Eo "${JIRA_PROJECT_KEY}-[0-9]+" \
    | sort -u \
    || true
}

add_merge_comment() {
  local issue_key=$1
  local comments
  local payload

  comments=$(request GET "rest/api/2/issue/${issue_key}/comment?maxResults=1000")
  # Workflow reruns should not add the same "Fixed in ..." comment repeatedly.
  if jq --exit-status --arg commit_url "${commit_url}" \
      'any(.comments[]?.body; contains($commit_url))' <<<"${comments}" >/dev/null; then
    echo "::notice::Jira issue ${issue_key} already has a comment for ${commit_url}"
    return
  fi

  payload=$(jq --null-input \
    --arg body "${jira_comment}" \
    '{body: $body}')
  request POST "rest/api/2/issue/${issue_key}/comment" "${payload}" >/dev/null
  echo "::notice::Added merge commit comment to Jira issue ${issue_key}"
}

resolve_issue() {
  local issue_key=$1
  local issue
  local resolution
  local status
  local transition_id
  local transitions
  local payload

  issue=$(request GET "rest/api/2/issue/${issue_key}?fields=status,resolution")
  status=$(jq --raw-output '.fields.status.name // ""' <<<"${issue}")
  resolution=$(jq --raw-output '.fields.resolution.name // ""' <<<"${issue}")

  # Release managers close resolved issues later, so this automation stops at
  # Resolved and treats already-Closed issues as successfully handled.
  if [[ "${status}" == 'Resolved' || "${status}" == 'Closed' ]]; then
    echo "::notice::Jira issue ${issue_key} is already ${status}"
    return
  fi

  transitions=$(request GET "rest/api/2/issue/${issue_key}/transitions?expand=transitions.fields")
  # Jira transition ids are workflow-specific. Prefer a transition whose target
  # status is Resolved, then fall back to a transition named like "Resolve Issue".
  transition_id=$(jq --raw-output '
    ([.transitions[]
      | select((.to.name // "" | ascii_downcase) == "resolved")
      | .id][0])
    // ([.transitions[]
      | select((.name // "" | ascii_downcase | test("resolve")))
      | .id][0])
    // empty
  ' <<<"${transitions}")

  if [[ -z "${transition_id}" ]]; then
    echo "::error::No Jira transition to Resolved was available for ${issue_key}"
    return 1
  fi

  payload=$(jq --null-input \
    --arg transition_id "${transition_id}" \
    --arg resolution_name "${JIRA_RESOLUTION_NAME}" \
    '{
      transition: {
        id: $transition_id
      },
      fields: {
        resolution: {
          name: $resolution_name
        }
      }
    }')

  # Some Jira workflows require resolution during the transition; others set it
  # through a transition screen and reject explicit field updates. Try the
  # explicit Fixed resolution first, then retry with only the transition id.
  if ! request POST "rest/api/2/issue/${issue_key}/transitions" "${payload}" >/dev/null; then
    echo "::warning::Could not transition ${issue_key} with resolution '${JIRA_RESOLUTION_NAME}'; retrying without explicit resolution"
    payload=$(jq --null-input \
      --arg transition_id "${transition_id}" \
      '{
        transition: {
          id: $transition_id
        }
      }')
    request POST "rest/api/2/issue/${issue_key}/transitions" "${payload}" >/dev/null
  fi

  issue=$(request GET "rest/api/2/issue/${issue_key}?fields=status,resolution")
  status=$(jq --raw-output '.fields.status.name // ""' <<<"${issue}")
  resolution=$(jq --raw-output '.fields.resolution.name // ""' <<<"${issue}")

  if [[ "${status}" != 'Resolved' && "${status}" != 'Closed' ]]; then
    echo "::error::Jira issue ${issue_key} transitioned to '${status}', not Resolved"
    return 1
  fi

  if [[ "${resolution}" != "${JIRA_RESOLUTION_NAME}" ]]; then
    echo "::warning::Jira issue ${issue_key} is ${status}, but its resolution is '${resolution:-unset}'"
  fi

  echo "::notice::Resolved Jira issue ${issue_key}"
}

require_command curl
require_command grep
require_command jq
require_command sort

: "${GITHUB_EVENT_PATH:?GITHUB_EVENT_PATH is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

JIRA_BASE_URL=${JIRA_BASE_URL:-https://issues.apache.org/jira}
JIRA_PROJECT_KEY=${JIRA_PROJECT_KEY:-CALCITE}
JIRA_RESOLUTION_NAME=${JIRA_RESOLUTION_NAME:-Fixed}

if ! [[ "${JIRA_PROJECT_KEY}" =~ ^[A-Z][A-Z0-9]+$ ]]; then
  echo "::error::JIRA_PROJECT_KEY must be an uppercase Jira project key"
  exit 1
fi

merged=$(jq --raw-output '.pull_request.merged // false' "${GITHUB_EVENT_PATH}")
if [[ "${merged}" != 'true' ]]; then
  echo "::notice::Pull request was closed without merge; skipping Jira update"
  exit 0
fi

pr_title=$(jq --raw-output '.pull_request.title // ""' "${GITHUB_EVENT_PATH}")
pr_body=$(jq --raw-output '.pull_request.body // ""' "${GITHUB_EVENT_PATH}")
pr_number=$(jq --raw-output '.pull_request.number // .number // ""' "${GITHUB_EVENT_PATH}")
pr_author=$(jq --raw-output '.pull_request.user.login // ""' "${GITHUB_EVENT_PATH}")
pr_url=$(jq --raw-output '.pull_request.html_url // ""' "${GITHUB_EVENT_PATH}")
merge_commit_sha=$(jq --raw-output '.pull_request.merge_commit_sha // ""' "${GITHUB_EVENT_PATH}")

# For Calcite, GitHub's UI is configured for rebase merges. The pull request
# event still exposes merge_commit_sha for the resulting commit; base.sha is a
# last-resort fallback if GitHub omits it.
if [[ -z "${merge_commit_sha}" || "${merge_commit_sha}" == 'null' ]]; then
  merge_commit_sha=$(jq --raw-output '.pull_request.base.sha // ""' "${GITHUB_EVENT_PATH}")
fi

if [[ -z "${merge_commit_sha}" || "${merge_commit_sha}" == 'null' ]]; then
  echo "::error::Could not determine the merge commit SHA"
  exit 1
fi

issue_keys=()
while IFS= read -r issue_key; do
  issue_keys+=("${issue_key}")
done < <(extract_issue_keys "${pr_title}")

# Only fall back to the PR body if the title has no Jira key. This avoids
# resolving incidental issue references when the title identifies the main fix.
if ((${#issue_keys[@]} == 0)); then
  while IFS= read -r issue_key; do
    issue_keys+=("${issue_key}")
  done < <(extract_issue_keys "${pr_body}")
fi

if ((${#issue_keys[@]} == 0)); then
  echo "::notice::No ${JIRA_PROJECT_KEY} Jira issue key was found; skipping Jira update"
  exit 0
fi

# ASF Jira uses basic authentication in this workflow.
if [[ -n "${JIRA_USER:-}" && -n "${JIRA_PASSWORD:-}" ]]; then
  jira_auth_args=(-u "${JIRA_USER}:${JIRA_PASSWORD}")
else
  echo "::error::Configure JIRA_USER and JIRA_PASSWORD secrets"
  exit 1
fi

commit_url="https://github.com/${GITHUB_REPOSITORY}/commit/${merge_commit_sha}"
jira_comment="Fixed in ${commit_url}"

if [[ -n "${pr_url}" ]]; then
  jira_comment="${jira_comment} via ${pr_url}"
elif [[ -n "${pr_number}" ]]; then
  jira_comment="${jira_comment} via PR #${pr_number}"
fi

if [[ -n "${pr_author}" ]]; then
  jira_comment="${jira_comment}. Thanks @${pr_author} for the contribution."
else
  jira_comment="${jira_comment}."
fi

for issue_key in "${issue_keys[@]}"; do
  add_merge_comment "${issue_key}"
  resolve_issue "${issue_key}"
done
