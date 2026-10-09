# Human merge handoff

Run `python3 scripts/agentic/finish.py PR --review-directory PATH` to assess the published current-head COMMENT review and successful required checks. It prints findings and the exact human merge command. The helper does not merge. The maintainer decides whether material findings, scientific evidence and outstanding conversations are resolved.

After a human merge, inspect the registered worktree and matching tip. Preserve ignored/untracked artifacts before removing it. Cleanup remains manual; no blanket deletion or automatic archival campaign is required.
