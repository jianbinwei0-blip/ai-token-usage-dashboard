#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP="${AI_USAGE_APP:-$HOME/Applications/AI Usage Island.app}"
OUTPUT="${1:-$ROOT/.artifacts/smoke-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUTPUT"
if [[ -e "$OUTPUT/smoke.json" ]]; then
  printf 'Choose a fresh smoke output directory; an old report already exists.\n' >&2
  exit 2
fi
# Stop only this app's exact executable, never unrelated terminals or agents.
python3 - "$APP/Contents/MacOS/UsageIsland" <<'PY'
import os,signal,subprocess,sys,time
executable=sys.argv[1]
rows=subprocess.check_output(["ps","-axo","pid=,comm="],text=True).splitlines()
pids=[int(parts[0]) for row in rows if len(parts:=row.strip().split(None,1))==2 and parts[1]==executable]
for pid in pids:
 try: os.kill(pid,signal.SIGTERM)
 except ProcessLookupError: pass
for _ in range(30):
 if not any(subprocess.run(["kill","-0",str(pid)],capture_output=True).returncode==0 for pid in pids): break
 time.sleep(.1)
else: sys.exit("The app did not quit; close it before running the smoke check.")
PY
open "$APP" --args --smoke-test "$OUTPUT"
python3 - "$OUTPUT" <<'PY'
import json,pathlib,sys,time
directory=pathlib.Path(sys.argv[1]); report=directory/"smoke.json"
for _ in range(225):
 if report.exists():
  data=json.loads(report.read_text())
  print(json.dumps(data,indent=2))
  print("Own-view captures:",directory)
  sys.exit(0 if data["passed"] else 1)
 time.sleep(.2)
sys.exit("The app did not produce a smoke report within 45 seconds.")
PY
