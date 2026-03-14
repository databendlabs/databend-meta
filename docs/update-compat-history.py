#!/usr/bin/env python3
"""Append a compatibility row to grpc-compat-history.txt for the current version.

Reads the workspace version from Cargo.toml, reduces it to MAJOR.0.0
(minor/patch don't affect compatibility), reads MIN_CLIENT_VERSION and
MIN_SERVER_VERSION from the version crate's Cargo.toml metadata, and
appends a new line if this major version isn't already recorded.
"""

import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORY_FILE = Path(__file__).resolve().parent / "grpc-compat-history.txt"
WORKSPACE_CARGO = REPO_ROOT / "Cargo.toml"
VERSION_CARGO = REPO_ROOT / "crates" / "version" / "Cargo.toml"


def read_toml(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def major_version(version_str: str) -> str:
    major = version_str.split(".")[0]
    return f"{major}.0.0"


def main():
    workspace = read_toml(WORKSPACE_CARGO)
    raw_version = workspace["workspace"]["package"]["version"]
    version = major_version(raw_version)

    compat = read_toml(VERSION_CARGO)["package"]["metadata"]["compat"]
    min_client = compat["min-client-version"]
    min_server = compat["min-server-version"]

    # Read existing history
    lines = HISTORY_FILE.read_text().splitlines()

    # Check if this major version already exists
    for line in lines:
        if line.strip().startswith(version):
            print(f"{version} already recorded, nothing to do.")
            return

    # Format: align columns to match existing rows
    new_line = f"{version:<16}{min_client:<19}{min_server}"
    lines.append(new_line)

    HISTORY_FILE.write_text("\n".join(lines) + "\n")
    print(f"Appended: {new_line}")


if __name__ == "__main__":
    main()
