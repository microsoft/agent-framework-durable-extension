# Copyright (c) Microsoft. All rights reserved.

"""Configure shared-helper assertions and test-owned deployment isolation."""

from collections.abc import Iterator
from pathlib import Path

import pytest

for support in Path(__file__).parent.parent.glob("*/tests/_*test_support*.py"):
    pytest.register_assert_rewrite(support.stem)


@pytest.fixture(scope="session", autouse=True)
def isolated_test_deployment() -> Iterator[None]:
    """Acknowledge test-owned isolation while allowing individual gate tests to override it."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "isolated_v2")
        yield
