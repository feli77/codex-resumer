## Engineering guardrails

- Prefer the simplest design that satisfies the current requirements. Avoid speculative abstractions, premature extensibility, and infrastructure without a demonstrated need.
- Keep verification proportional to the change and its risk. Run the smallest relevant test set first; expand testing only when failures, shared dependencies, or regression risk justify it. Do not run large test suites without a clear validation purpose.
- Avoid introducing hashes, content-derived identifiers, hash-based cache keys, or deduplication schemes unless the requirement genuinely depends on them and a simpler stable identifier is insufficient.

## Agent skills

### Issue tracker

Issues and specs are tracked in GitHub Issues for `feli77/codex-resumer`. See `docs/agents/issue-tracker.md`.

After implementing an issue, close it only when every acceptance criterion and required verification has passed. Post a final comment summarizing the implementation and verification evidence before closing. Close a parent issue only after all child issues and parent-level acceptance are complete.

### Triage labels

Triage uses the five default canonical labels. See `docs/agents/triage-labels.md`.

### Domain docs

Domain documentation uses the single-context layout. See `docs/agents/domain.md`.
