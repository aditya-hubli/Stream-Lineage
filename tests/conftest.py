"""Shared test configuration and path setup."""

import os
import sys

# Ensure SDK is importable from tests
sdk_path = os.path.join(os.path.dirname(__file__), "..", "sdk")
sys.path.insert(0, sdk_path)

# Ensure service modules are importable
ingester_path = os.path.join(os.path.dirname(__file__), "..", "services", "lineage-ingester")
sys.path.insert(0, ingester_path)
