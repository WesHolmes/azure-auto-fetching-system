# Git Workflow

## Quick Reference

1. Branch from main
2. Do your work
3. Merge main into your branch locally
4. Push and create PR
5. Never push directly to main

## Commands

### Start Feature

```bash
git checkout main && git pull
git checkout -b feature/your-name
```

### Before Creating PR

```bash
# Update main
git checkout main && git pull

# Merge main into your branch
git checkout feature/your-name
git merge main

# Fix conflicts if any
# Test your changes

# Push to remote
git push origin your-initials/feature/[name]
```

### Create PR

- Go to GitHub
- Click "New pull request"
- Base: `main` ← Compare: `your-initials/feature/[name]`
- Add reviewers
- Submit

## GitHub Branch Protection Setup

### Configure Main Branch Protection

1. Repository Settings → Branches
2. Add rule → Branch name pattern: `main`
3. Enable:
   - ✓ Require a pull request before merging
   - ✓ Require approvals: 1
   - ✓ Dismiss stale pull request approvals when new commits are pushed
   - ✓ Include administrators
   - ✓ Require branches to be up to date before merging
4. Save changes

### Result

- No direct pushes to main
- All changes require PR + review
- Admins also follow rules
- Forces merge conflict resolution before PR

## Common Issues

### Conflict Resolution

```bash
git merge main
# Fix conflicts in VS Code
git add .
git commit -m "Resolve merge conflicts"
git push origin feature/your-name
```

### Accidental Main Push (Blocked)

```
! [remote rejected] main -> main (protected branch hook declined)
```

Good! Create a feature branch and PR instead.

### PR Shows "Out of Date"

Your branch needs main's latest changes:

```bash
git checkout main && git pull
git checkout feature/your-name
git merge main
git push origin feature/your-name
```

## Team Rules

- One feature per branch
- Descriptive branch names: `feature/user-sync`, not `feature/stuff`
- Keep PRs small and focused
- Update from main frequently to avoid conflicts

## Git Workflow for Beginners

### Initial Setup
```bash
# Configure Git (first time only)
git config --global user.name "Your Name"
git config --global user.email "your.email@company.com"

# Clone the repository
git clone https://github.com/yourcompany/fa-op-sync.git
cd fa-op-sync
```

### Daily Development Workflow
```bash
# 1. Always start with latest code
git checkout main
git pull origin main

# 2. Create a feature branch
git checkout -b feature/add-email-validation

# 3. Make your changes
# Edit files...

# 4. Check what changed
git status
git diff

# 5. Stage and commit changes
git add .
git commit -m "Add email validation to user creation"

# 6. Push to remote
git push origin feature/add-email-validation

# 7. Create Pull Request on GitHub
# - Go to repository on GitHub
# - Click "Pull requests" > "New pull request"
# - Select your branch
# - Add description and submit
```

### Commit Message Examples
Good commit messages:
- "Add email validation to user creation endpoint"
- "Fix error handling in delete user function"
- "Update Graph API client to handle token expiration"
- "Remove unused imports from function_app.py"

Bad commit messages:
- "Fixed stuff"
- "Updates"
- "asdfasdf"
- "Working on user feature"

### Common Git Commands
```bash
# See what branch you're on
git branch

# See recent commits
git log --oneline -5

# Discard local changes to a file
git checkout -- filename.py

# See all branches (including remote)
git branch -a

# Switch to existing branch
git checkout branch-name

# Delete local branch (after PR is merged)
git branch -d feature/branch-name
```