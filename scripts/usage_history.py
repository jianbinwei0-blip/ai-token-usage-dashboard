#!/usr/bin/env python3
"""Validate, export or explicitly restore observed history without collecting usage."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dashboard_core.config import DashboardConfig
from dashboard_core.history import HistoryError, export_history, read_history, restore_history


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history-file', type=Path, help='Override the configured durable history location')
    commands = parser.add_subparsers(dest='command', required=True)
    validate = commands.add_parser('validate', help='Validate an existing backup without writing')
    validate.add_argument('file', type=Path)
    export = commands.add_parser('export', help='Write a validated backup to a NEW file')
    export.add_argument('file', type=Path)
    restore = commands.add_parser('restore', help='Restore a validated backup; stop collectors first')
    restore.add_argument('file', type=Path)
    restore.add_argument('--replace', action='store_true', help='Preserve existing history in a backup, then replace it')
    args = parser.parse_args()
    try:
        if args.command == 'validate':
            read_history(args.file)
            print('History is valid. No files changed.')
            return 0
        target = args.history_file or DashboardConfig.from_env(Path(__file__).resolve().parents[1]).parse_cache_file
        if target is None:
            raise HistoryError('No durable history file is configured.')
        if args.command == 'export':
            export_history(target, args.file)
            print('Validated history exported. Treat the backup as private local data.')
        else:
            backup = restore_history(args.file, target, replace=args.replace)
            print('Validated history restored.' + (' Previous history was backed up beside the destination.' if backup else ''))
        return 0
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
