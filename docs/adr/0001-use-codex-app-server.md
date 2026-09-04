# Use Codex App Server as the integration boundary

Codex Resumer integrates through Codex App Server rather than parsing human-readable CLI output because reliable scheduling requires structured usage-limit, reset-time, Turn, interruption, and approval events. Because the protocol is still evolving, the service probes required capabilities at startup and has no automatic CLI fallback in the MVP.
