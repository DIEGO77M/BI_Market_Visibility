# 📖 Quick Reference Guide

Quick command reference for daily development workflow with Databricks, VS Code, and GitHub.

---

## 🚀 Quick Start (First Time Setup)

```bash
# 1. Clone repository
git clone https://github.com/DIEGO77M/BI_Market_Visibility.git
cd BI_Market_Visibility

# 2. Create virtual environment
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure Databricks
databricks configure --token
# Host: https://dbc-fd5c2cc6-9b83.cloud.databricks.com
# Token: [your PAT token]

# 5. Validate setup
databricks bundle validate
```

---

## 📝 Daily Workflow Commands

### Git Workflow
```bash
# Start working on new feature
git checkout develop
git pull origin develop
git checkout -b feature/your-feature-name

# Make changes, then commit
git add .
git commit -m "feat: Your descriptive message"
git push -u origin feature/your-feature-name

# Create Pull Request on GitHub UI
# After approval and merge, update local
git checkout develop
git pull origin develop
git branch -d feature/your-feature-name  # Delete local branch
```

### Databricks Deployment
```bash
# Deploy code to Databricks workspace
databricks bundle deploy

# Validate configuration
databricks bundle validate

# List deployed files
databricks workspace list /Workspace/Users/diego.mayorgacapera@gmail.com/.bundle/BI_Market_Visibility/dev/files

# Export notebook from Databricks to local
databricks workspace export /Workspace/.../notebook.py ./local_notebook.py
```

### Python Development
```bash
# Activate virtual environment
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux

# Run tests
pytest src/tests/ -v

# Run tests with coverage
pytest src/tests/ --cov=src --cov-report=html

# Format code
black src/

# Lint code
flake8 src/

# Sort imports
isort src/
```

---

## 🔍 Useful Commands

### Databricks CLI
```bash
# List workspace contents
databricks workspace ls /path/to/folder

# Export file
databricks workspace export <remote_path> <local_path>

# Import file
databricks workspace import <local_path> <remote_path>

# List DBFS files
databricks fs ls dbfs:/path

# Copy to DBFS
databricks fs cp <local_path> dbfs:/path

# List clusters
databricks clusters list

# Get cluster info
databricks clusters get --cluster-id <id>
```

### Git Commands
```bash
# Check status
git status

# View commit history
git log --oneline --graph --all

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Discard local changes
git checkout -- <file>
git restore <file>  # Git 2.23+

# Update from remote
git fetch origin
git pull origin develop

# Create and switch branch
git checkout -b branch-name

# Switch branch
git checkout branch-name

# Delete branch
git branch -d branch-name  # Local
git push origin --delete branch-name  # Remote

# View remote info
git remote -v

# View branch info
git branch -a
```

### VS Code Shortcuts
```bash
# Command Palette
Ctrl+Shift+P  # Windows/Linux
Cmd+Shift+P   # macOS

# Quick Open File
Ctrl+P / Cmd+P

# Toggle Terminal
Ctrl+`  # (backtick)

# Search in Files
Ctrl+Shift+F / Cmd+Shift+F

# Go to Definition
F12

# Find References
Shift+F12

# Format Document
Shift+Alt+F  # Windows/Linux
Shift+Option+F  # macOS

# Multi-cursor
Alt+Click  # Windows/Linux
Option+Click  # macOS
```

---

## 🐛 Troubleshooting Quick Fixes

### Databricks Connection Issues
```bash
# Test connection
databricks workspace list /Users/diego.mayorgacapera@gmail.com

# Reconfigure if failed
databricks configure --token

# Check config file
cat ~/.databrickscfg  # macOS/Linux
type %USERPROFILE%\.databrickscfg  # Windows
```

### Git Issues
```bash
# Reset to remote state (CAREFUL!)
git fetch origin
git reset --hard origin/develop

# Stash changes temporarily
git stash
git stash pop  # Apply stashed changes back

# Merge conflicts
git status  # See conflicted files
# Edit files to resolve conflicts
git add <resolved_files>
git commit
```

### Python Environment
```bash
# Recreate virtual environment
deactivate
rm -rf venv  # Linux/macOS
rmdir /s venv  # Windows
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt

# Update all packages
pip list --outdated
pip install --upgrade pip
pip install -r requirements.txt --upgrade
```

---

## 📋 Commit Message Conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
feat: Add new feature
fix: Fix bug
docs: Update documentation
style: Code style changes (formatting)
refactor: Code refactoring
test: Add or update tests
chore: Maintenance tasks
ci: CI/CD changes
perf: Performance improvements
```

**Examples:**
```bash
git commit -m "feat: Add bronze layer ingestion for PDV data"
git commit -m "fix: Correct date parsing in silver transformation"
git commit -m "docs: Update architecture diagram with new layers"
git commit -m "test: Add unit tests for data quality checks"
git commit -m "refactor: Simplify spark helper functions"
```

---

## 🔐 Environment Variables

Create `.env` file (already in `.gitignore`):
```bash
# Databricks
DATABRICKS_HOST=https://dbc-fd5c2cc6-9b83.cloud.databricks.com
DATABRICKS_TOKEN=dapi123...  # For CI/CD only, use .databrickscfg locally

# Python
PYTHONPATH=./src

# Spark
SPARK_HOME=/path/to/spark
PYSPARK_PYTHON=python3
```

Load in Python:
```python
from dotenv import load_dotenv
import os

load_dotenv()
host = os.getenv("DATABRICKS_HOST")
```

---

## 📊 Project Status Check

```bash
# Git status
git status
git branch
git log --oneline -5

# Databricks connection
databricks bundle validate

# Python environment
python --version
pip list | grep -E "(pyspark|databricks|pytest)"

# Tests
pytest src/tests/ --collect-only

# Code quality
flake8 src/ --count --statistics
```

---

## 🎯 Pre-Push Checklist

Before pushing code:
```bash
# 1. Run tests
pytest src/tests/ -v

# 2. Check code style
black --check src/
flake8 src/

# 3. Validate Databricks bundle
databricks bundle validate

# 4. Check Git status
git status

# 5. Review changes
git diff

# 6. Commit with clear message
git commit -m "type: clear description"

# 7. Push
git push origin your-branch-name

# 8. Create Pull Request on GitHub
```

---

## 📞 Getting Help

| Issue | Command / Resource |
|-------|-------------------|
| Databricks CLI help | `databricks --help` |
| Git help | `git --help` or `git <command> --help` |
| Python package help | `pip show <package>` |
| VS Code docs | Press `F1` → "Help: Welcome" |
| Project issues | https://github.com/DIEGO77M/BI_Market_Visibility/issues |
| Full setup guide | [docs/DEVELOPMENT_SETUP.md](DEVELOPMENT_SETUP.md) |
| Architecture docs | [docs/INTEGRATION_ARCHITECTURE.md](INTEGRATION_ARCHITECTURE.md) |

---

## 🔗 Quick Links

- **Repository:** https://github.com/DIEGO77M/BI_Market_Visibility
- **Databricks Workspace:** https://dbc-fd5c2cc6-9b83.cloud.databricks.com
- **CI/CD Pipeline:** https://github.com/DIEGO77M/BI_Market_Visibility/actions
- **Issues:** https://github.com/DIEGO77M/BI_Market_Visibility/issues
- **Pull Requests:** https://github.com/DIEGO77M/BI_Market_Visibility/pulls

---

**💡 Pro Tip:** Bookmark this page for quick reference during development!

**📌 Keep this file updated** as you discover new useful commands or workflows.
