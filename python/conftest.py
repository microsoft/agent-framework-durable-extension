# Copyright (c) Microsoft. All rights reserved.

"""Declare isolation for test-owned hubs without changing production defaults.

Deployment gate tests explicitly remove or replace this acknowledgement with
their function-scoped monkeypatch to verify missing and invalid configurations.
"""

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolated_test_deployment() -> Iterator[None]:
    """Acknowledge test deployment isolation and restore the environment afterward."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "isolated_v2")
        yield
