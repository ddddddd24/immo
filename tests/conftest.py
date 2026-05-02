"""Pytest fixtures + env stubs for the immo bot test suite."""
import os
import sys
from pathlib import Path

# ─── Env stubs MUST be set before any project module imports ─────────────────
# config.py raises EnvironmentError on missing required keys when MOCK_MODE is
# off. Tests run with MOCK_MODE=true so the LLM client is None and side-effects
# are blocked, then individual tests opt back into specific behaviour by
# monkey-patching.

_TEST_ENV = {
    "MOCK_MODE": "true",
    "ANTHROPIC_API_KEY": "stub",
    "TELEGRAM_BOT_TOKEN": "stub",
    "TELEGRAM_CHAT_ID": "stub",
    "APIFY_API_KEY": "stub",
    "LBC_EMAIL": "stub@stub.com",
    "LBC_PASSWORD": "stub",
    "USE_DEEPSEEK": "false",
}
for k, v in _TEST_ENV.items():
    os.environ.setdefault(k, v)

# Make project root importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
FIXTURE_DIR = PROJECT_ROOT / "data"

import pytest  # noqa: E402


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Per-test SQLite DB. Patches config.DB_PATH and re-runs init_db."""
    db_path = tmp_path / "test.db"
    import config
    import database
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    database.init_db()
    return database


@pytest.fixture
def fixture_html():
    """Returns a callable that loads a captured HTML page from data/."""
    def _load(name: str) -> str:
        path = FIXTURE_DIR / name
        if not path.exists():
            pytest.skip(f"fixture {name} missing")
        return path.read_text(encoding="utf-8")
    return _load


@pytest.fixture
def make_listing():
    """Factory for a Listing dataclass with sensible defaults."""
    from agent import Listing

    def _make(**overrides) -> Listing:
        defaults = dict(
            lbc_id="t_test_1",
            title="Studio meublé 28m² Paris 11",
            description="Beau studio. Charges comprises.",
            price=850,
            location="Paris 75011",
            seller_name="Marie Test",
            url="https://example.com/listing/1",
            seller_type_hint="",
            source="leboncoin",
            images=[],
        )
        defaults.update(overrides)
        return Listing(**defaults)

    return _make
