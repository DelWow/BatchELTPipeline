"""Command-line interface for narrow pipeline entry points."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from housing_elt.config import SettingsError, load_settings


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser without reading configuration or changing state."""
    parser = argparse.ArgumentParser(
        prog="housing-elt",
        description="Canadian metropolitan housing batch ELT pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "show-config",
        help="print resolved non-secret local configuration as JSON",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run a selected command and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "show-config":
        try:
            settings = load_settings()
        except SettingsError as error:
            parser.error(str(error))

        print(json.dumps(settings.to_display_dict(), indent=2, sort_keys=True))
        return 0

    # argparse enforces the command choices, so reaching this branch would be a
    # programming error rather than invalid user input.
    raise AssertionError(f"Unhandled command: {args.command}")
