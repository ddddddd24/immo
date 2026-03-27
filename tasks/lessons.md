# Lessons Learned

---

## 2026-03-24 — Skipped CLAUDE.md workflow on first build

### What happened
Given a large, fully-specced build task (10+ files, multiple integrations), I jumped
straight into implementation without following the workflow defined in CLAUDE.md.

### Rules I violated
1. **Plan first** — should have used `EnterPlanMode`, explored the codebase/specs,
   and written a plan to `tasks/todo.md` before writing a single line of code.
2. **Verify plan** — should have presented the plan and waited for user sign-off.
3. **Subagents** — should have offloaded file creation / parallel work to subagents
   to keep the main context window clean.
4. **tasks/todo.md** — used the in-memory `TodoWrite` tool instead of the actual file.
5. **Review section** — never added a post-build review to `tasks/todo.md`.
6. **tasks/lessons.md** — never created this file (captured here retroactively).

### Root cause
Detailed specs felt like implicit approval to start. They are not — specs describe
*what* to build, not approval to skip the planning workflow.

### Rule going forward
> **Any task with 3+ steps or multiple files → EnterPlanMode first, no exceptions.**
> Write the plan to `tasks/todo.md`. Wait for explicit user approval. Only then implement.
> After completion, always add a Review section to `tasks/todo.md` and update this file.
