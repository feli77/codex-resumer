# Use SQLite for durable state

Codex Resumer stores Queue and Task state in SQLite rather than JSON files because crash recovery requires atomic state transitions and reliable reconciliation after restart. SQLite provides those guarantees in a single local file without introducing an external database service.
