#!/bin/bash
# Data lives outside ~/Downloads so launchd sync agents can read it
# (macOS TCC gates Downloads for agents by default). The old path is
# preserved as a symlink for backward-compat.
export TT_CAPTURES_DIR="$HOME/ttl-data/captures"
cd "/Users/davidzilinski/Downloads/TT recorder live"
source venv/bin/activate
exec python3 dashboard.py
