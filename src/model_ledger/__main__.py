from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

from .experiment import demo, evaluate
from .registry import LedgerError


def main(argv=None):
    parser = argparse.ArgumentParser(description="Local content-addressed artifact registry and lifecycle experiments")
    parser.add_argument("command", choices=("demo", "evaluate"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            with tempfile.TemporaryDirectory() as directory:
                result = demo(directory)
        else:
            result = evaluate()
        encoded = json.dumps(result, indent=2, allow_nan=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(encoded + "\n")
        print(encoded)
        return 2 if result.get("passed", 1) != result.get("case_count", 1) else 0
    except (LedgerError, OSError, sqlite3.Error) as error:
        print(json.dumps({"error": getattr(error, "code", "registry_error"), "detail": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
