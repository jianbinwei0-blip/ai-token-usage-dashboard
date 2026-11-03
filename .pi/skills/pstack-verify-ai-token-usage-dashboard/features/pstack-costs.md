# Spend and model breakdown

A user sees correctly scoped spend and actual agent/model rows, with no false
partial-price marker for the fully priced synthetic fixture.

## Sub-features

- pstack-spend shows the rounded sum of supported fixture override prices.
- pstack-models shows the models for the chosen provider/window.
- pstack-completeness distinguishes a genuinely complete fixture price result.

## How to get to it (user POV)

Read Spend in the executive summary and the Agent CLI + Model breakdown below
the daily table; switch providers or date controls to scope those values.

## Driving it with pstack dashboard harness

Preconditions: Doctor passes; use the owned synthetic pricing override.

- **Drive.** Run `node "$W" "$RUN"` once for refresh/filter/cost coverage.
- **Combined.** Baseline All Available has 2,410,000 tokens, exact spend $2.269,
  visible spend $2.27, and gpt-5.4 plus claude-sonnet-4-6 rows.
- **PI.** The PI view has only the gpt-5.4 fixture model and baseline spend $0.31.
- **Completeness.** Require all dataset rows priced, no partial marker in Spend,
  and real UI values equal to the independent fixture ledger.
- **Evidence.** Retain API price summaries, DOM snapshots including model text,
  and the combined/PI screenshots.

## Gotchas

- These fixed override prices prove arithmetic/config propagation, not live
  invoices or current vendor rates. Existing unit tests cover built-in rates.
- The CLI deliberately rounds currency more aggressively and defaults to MTD;
  do not compare its compact string with an All Available web value.
- Unknown-model partial pricing has separate unit coverage, not a live proof
  in this initial fixture. Do not claim the complete fixture tested that path.
