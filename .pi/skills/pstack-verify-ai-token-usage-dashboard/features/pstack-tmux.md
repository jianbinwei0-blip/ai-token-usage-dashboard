# Tmux status CLI

A terminal user gets a healthy, compact Today/MTD token and spend line, while
account lookups stay disabled and persistent cache metrics remain precise.

## Sub-features

- pstack-cli invokes the real renderer, not a copied formatter.
- pstack-width keeps output to one line within the requested 96 characters.
- pstack-cache confirms exact current-day and month-to-date metrics.
- pstack-account-off proves no account adapter was invoked.

## How to get to it (user POV)

The same scripts/render_tmux_status.py supplies tmux's status segment. No live
tmux session or tmux configuration change is needed to verify its output.

## Driving it with pstack dashboard harness

Preconditions: Doctor passes and the run has private runtime/cache paths.

- **Drive.** Run `python3 "$H" tmux "$RUN"`. It invokes the real CLI with
  --chatgpt-usage off, a private --cache-file, and --max-width 96.
- **Output.** Require exit 0, one healthy line with Today, MTD, and spend, and
  no error fallback. Exit 0 by itself is insufficient.
- **Precision.** Compare snapshot metrics to the fixture ledger. Fresh MTD is
  1,860,000 tokens and exact spend $1.759; Today follows the actual seed dates.
- **Account.** Require subscription_enabled:false, no subscription payload,
  and no invocation marker from the fail-fast Codex stub.
- **Evidence.** Retain pstack-tmux*.json with the command, stdout/stderr,
  exit status, exact metrics, and account-disabled state.

## Gotchas

- The real CLI returns exit 0 even for some error fallback strings; check the
  snapshot health and expected numbers.
- Compact token/currency strings are intentionally rounded. Exact cache values
  are the correctness oracle, not arbitrary precision in the status text.
- Never run the bare production README command for this QA; it defaults to
  real sources and may query the signed-in account.
- Live plan/quota, tmux styling variants, and the native island UI are not
  established by this isolated formatter proof.
