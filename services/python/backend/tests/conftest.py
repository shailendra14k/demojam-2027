import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]  # services/python
REPO_ROOT = Path(__file__).resolve().parents[4]  # casino-sentinel
for p in [
    ROOT,
    ROOT / "services" / "aml-rules",
    ROOT / "services" / "feature-engine",
    ROOT / "services" / "risk-engine",
    ROOT / "services" / "explainability",
    ROOT / "ml" / "training",
    ROOT / "ai" / "adapter",
    REPO_ROOT / "sql" / "scenarios",
]:
    sys.path.insert(0, str(p))


@pytest.fixture(scope="session")
def api_main():
    if "casino_api" in sys.modules:
        return sys.modules["casino_api"]
    spec = importlib.util.spec_from_file_location("casino_api", ROOT / "backend" / "main.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["casino_api"] = mod
    spec.loader.exec_module(mod)
    return mod
