# Project verification skills

This directory contains shared source, not local execution state. Its skill
definitions, feature recipes, helpers and safety tests are intended to travel
with the repository.

## Choose a surface

- [pstack-verify-ai-token-usage-dashboard](pstack-verify-ai-token-usage-dashboard/SKILL.md): real web controls, recalc API, retained session history and tmux status.
- [pstack-verify-usage-island](pstack-verify-usage-island/SKILL.md): native build, packaged adapter, synthetic account/reset details, freshness, resources, display presentation and app-local mouse/Escape flows.

Run both for shared collector, canonical formatter, pricing or account-contract
changes. Each skill has a maintained `features/README.md` and executable helpers.

In Pi, open this repository, run `/reload`, then invoke the matching skill:

```text
/skill:pstack-verify-ai-token-usage-dashboard
/skill:pstack-verify-usage-island
```

The launch/doctor/drive/cleanup shell recipes also work without Pi or an installed
pstack package. The web recipe needs Python 3.9+, Node.js 22+ and Chrome/Chromium.
Native verification needs macOS 14+, Swift 6/CLT and Python 3.9+; full input proof
needs an active unlocked desktop. Locked preview is rendering/privacy proof only.

## Source versus runtime policy

- Keep reviewed skill instructions, feature maps, scripts, tests and intentional
  reference assets here. Do not include credentials or personal machine paths.
- Keep generated fixtures, caches, run records, logs, screenshots, app bundles and
  HTML/JSON evidence in the helpers' owned run directories under repository `tmp/`.
- `.gitignore` allowlists `.pi/skills/`, but still ignores the rest of `.pi/` by
  default: tasks, schedules, local QA state/evidence and generated extensions.
- Python caches, logs and PID files stay ignored globally. Accidental
  `pstack-evidence/`, `pstack-scratch/` and `.artifacts/` directories inside skill
  trees are also ignored; these guards are not a reason to put runtime data here.
- Other durable Pi project resources need an intentional allowlist before sharing.
  Do not remove the runtime boundary or force-add private evidence.

## Check source changes

From the repository root:

```bash
python3 .pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_verify_tests.py
python3 .pi/skills/pstack-verify-usage-island/scripts/pstack_island_tests.py
node --check .pi/skills/pstack-verify-ai-token-usage-dashboard/scripts/pstack_web.mjs
```

Run the relevant skill's real user-flow sequence when its behavior changes;
unit/safety tests do not replace real UI evidence. Preserve failed and blocked
runs separately and confirm evidence survives cleanup. Review the feature maps
with `/skill:pstack-maintain-verification-skill` when app behavior changes.
