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
set -e

echo "🚀 Setting up Claude Code git worktree commands..."

# Clean up any existing experimental attempts
echo "🧹 Cleaning up previous attempts..."
rm -f ~/.claude/commands/git-tree*.md
rm -f ~/.claude/commands/merge-back*.md
rm -f ~/.claude/commands/cleanup-worktree*.md
rm -f ~/.claude/scripts/git-tree*.sh
rm -f ~/.claude/scripts/merge-back*.sh
rm -f ~/.claude/scripts/cleanup-worktree*.sh

# Create directories
mkdir -p ~/.claude/scripts
mkdir -p ~/.claude/commands

echo "📁 Created directories"

# Create git-tree.sh script
cat > ~/.claude/scripts/git-tree.sh << 'EOF'
#!/bin/bash
set -e

FEATURE_NAME="$1"
if [ -z "$FEATURE_NAME" ]; then
    echo "Usage: git-tree.sh 'feature description'"
    exit 1
fi

# Convert to branch name
BRANCH_NAME="feature/$(echo "$FEATURE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/ /-/g' | sed 's/[^a-z0-9-]//g')"
REPO_NAME=$(basename $(git rev-parse --show-toplevel))
WORKTREE_DIR="../${REPO_NAME}-$(echo "$FEATURE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/ /-/g' | sed 's/[^a-z0-9-]//g')"

echo "🌳 Creating worktree for: $FEATURE_NAME"
echo "📁 Branch: $BRANCH_NAME"
echo "📂 Directory: $WORKTREE_DIR"

# Create branch and worktree
git checkout -b "$BRANCH_NAME" 2>/dev/null || git checkout "$BRANCH_NAME"
git checkout main
git worktree add "$WORKTREE_DIR" "$BRANCH_NAME"

echo "✅ Worktree created successfully!"
echo "📍 Location: $WORKTREE_DIR"
EOF

# Create git-tree-start.sh script
cat > ~/.claude/scripts/git-tree-start.sh << 'EOF'
#!/bin/bash
set -e

FEATURE_NAME="$1"
REPO_NAME=$(basename $(git rev-parse --show-toplevel))
WORKTREE_DIR="../${REPO_NAME}-$(echo "$FEATURE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/ /-/g' | sed 's/[^a-z0-9-]//g')"

if [ ! -d "$WORKTREE_DIR" ]; then
    echo "❌ Worktree not found: $WORKTREE_DIR"
    echo "Run: /git-tree '$FEATURE_NAME' first"
    exit 1
fi

echo "🚀 Starting Claude in worktree: $WORKTREE_DIR"
cd "$WORKTREE_DIR"
exec claude -p "I'm working on implementing: $FEATURE_NAME. This is a git worktree environment. Let's start by understanding the current codebase structure and planning the implementation."
EOF

# Create merge-back.sh script
cat > ~/.claude/scripts/merge-back.sh << 'EOF'
#!/bin/bash
set -e

FEATURE_DESC="$1"
BRANCH_NAME=$(git branch --show-current)

echo "🔄 Merging feature back to main: $FEATURE_DESC"
echo "📋 Current branch: $BRANCH_NAME"

# Commit any pending changes
if ! git diff-index --quiet HEAD --; then
    echo "📝 Committing pending changes..."
    git add .
    git commit -m "feat: $FEATURE_DESC"
fi

# Push the branch
echo "🚀 Pushing feature branch..."
git push origin "$BRANCH_NAME"

# Create PR if gh is available
if command -v gh >/dev/null 2>&1; then
    echo "📝 Creating pull request..."
    gh pr create \
        --title "feat: $FEATURE_DESC" \
        --body "Implementation of: $FEATURE_DESC

Developed in git worktree: $(pwd)" \
        --base main
    echo "✅ Pull request created!"
else
    echo "⚠️ GitHub CLI not found. Create PR manually:"
    echo "   Branch: $BRANCH_NAME"
fi

echo ""
echo "After PR is merged, run: /cleanup-worktree"
EOF

# Create cleanup-worktree.sh script
cat > ~/.claude/scripts/cleanup-worktree.sh << 'EOF'
#!/bin/bash
set -e

CURRENT_PATH=$(pwd)
MAIN_REPO=$(git rev-parse --show-toplevel 2>/dev/null)

# Check if we're in a worktree
if [ "$CURRENT_PATH" = "$MAIN_REPO" ]; then
    echo "❌ You're in the main repository, not a worktree"
    echo "Navigate to a worktree directory first"
    exit 1
fi

BRANCH_NAME=$(git branch --show-current)

echo "🧹 Cleaning up worktree: $CURRENT_PATH"
echo "📋 Branch: $BRANCH_NAME"

# Go to main repo and remove worktree
cd "$MAIN_REPO"
git worktree remove "$CURRENT_PATH" --force

echo "✅ Worktree removed: $CURRENT_PATH"
echo "📁 Back in main repo: $(pwd)"
echo ""
echo "Optional cleanup (after confirming merge):"
echo "  git branch -d $BRANCH_NAME"
echo "  git push origin --delete $BRANCH_NAME"
EOF

# Make scripts executable
chmod +x ~/.claude/scripts/*.sh

echo "🔧 Created and made executable: bash scripts"

# Create git-tree command
cat > ~/.claude/commands/git-tree.md << 'EOF'
---
allowed-tools: Bash(~/.claude/scripts/git-tree.sh:*)
description: Create git worktree for feature development
---

Creating git worktree for: $ARGUMENTS

!`~/.claude/scripts/git-tree.sh "$ARGUMENTS"`

✅ Worktree created! Ready to implement: $ARGUMENTS

What aspect of this feature should I start with?
EOF

# Create git-tree-switch command
cat > ~/.claude/commands/git-tree-switch.md << 'EOF'
---
allowed-tools: Bash(~/.claude/scripts/git-tree-start.sh:*)
description: Switch to worktree and start new Claude session
---

Switching to worktree for: $ARGUMENTS

!`~/.claude/scripts/git-tree-start.sh "$ARGUMENTS"`
EOF

# Create merge-back command
cat > ~/.claude/commands/merge-back.md << 'EOF'
---
allowed-tools: Bash(~/.claude/scripts/merge-back.sh:*)
description: Merge feature back to main with PR
---

Merging back feature: $ARGUMENTS

!`~/.claude/scripts/merge-back.sh "$ARGUMENTS"`
EOF

# Create cleanup-worktree command
cat > ~/.claude/commands/cleanup-worktree.md << 'EOF'
---
allowed-tools: Bash(~/.claude/scripts/cleanup-worktree.sh:*)
description: Clean up completed worktree
---

Cleaning up current worktree...

!`~/.claude/scripts/cleanup-worktree.sh`
EOF

echo "📝 Created Claude commands"

# Show final status
echo ""
echo "✅ Setup complete! Claude Code git worktree commands are now available:"
echo ""
echo "  /git-tree 'feature description'     - Create worktree for feature"
echo "  /git-tree-switch 'feature name'     - Switch to worktree and start new session"
echo "  /merge-back 'feature description'   - Commit, push, and create PR"
echo "  /cleanup-worktree                   - Clean up current worktree"
echo ""
echo "Files created:"
echo "  ~/.claude/scripts/git-tree.sh"
echo "  ~/.claude/scripts/git-tree-start.sh"
echo "  ~/.claude/scripts/merge-back.sh"
echo "  ~/.claude/scripts/cleanup-worktree.sh"
echo "  ~/.claude/commands/git-tree.md"
echo "  ~/.claude/commands/git-tree-switch.md"
echo "  ~/.claude/commands/merge-back.md"
echo "  ~/.claude/commands/cleanup-worktree.md"
echo ""
echo "🎉 Ready to use! Try: /git-tree 'my new feature'"
