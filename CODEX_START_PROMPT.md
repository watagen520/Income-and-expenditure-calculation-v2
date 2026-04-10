# CODEX_START_PROMPT.md

Use the current GitHub repository as the source of truth.

Read all of these before doing any work:
- README.md
- README_IMPLEMENTATION.md
- AGENTS.md
- PROJECT_OVERVIEW.md
- SYSTEM_FACTS.md
- CURRENT_PROBLEMS.md
- TARGET_STATE.md
- DATA_SEMANTICS.md
- REPAIR_SCOPE.md
- ACCEPTANCE_TESTS.md
- TASK.md

This is a repair task, not a greenfield implementation.

The user edits in VSCode, pushes to GitHub, and pulls from Databricks Repos.
Keep that workflow intact.

Do not start coding immediately.
First:
1. summarize your understanding
2. identify root causes
3. map the defects to likely code locations
4. propose the smallest safe repair plan
5. explain how your plan satisfies ACCEPTANCE_TESTS.md

Only then proceed with implementation.