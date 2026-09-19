# Copyright (c) Microsoft. All rights reserved.

"""Declare isolated deployment mode for the local test hubs.

This pytest-only fixture explicitly acknowledges the tests' isolated mode. It does
not bypass the production default. Gate tests remove or replace the variable with
their function-scoped monkeypatch fixture to exercise missing and invalid modes.
"""

from collections.abc import Iterator

import pytest


@pytest.fixture(scope="session", autouse=True)
def isolated_test_deployment() -> Iterator[None]:
    """Declare isolated mode for tests and restore the environment at session end."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "isolated_v2")
        yield