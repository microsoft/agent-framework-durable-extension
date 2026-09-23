# Copyright (c) Microsoft. All rights reserved.

"""Keep test deployment setup visible when pytest selects this package's config."""

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolated_test_deployment() -> Iterator[None]:
    """Acknowledge test-owned isolation while allowing individual gate tests to override it."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "isolated_v2")
        yield
