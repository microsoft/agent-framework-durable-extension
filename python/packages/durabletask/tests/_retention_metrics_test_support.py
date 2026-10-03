# Copyright (c) Microsoft. All rights reserved.

"""Isolated retention metric fixtures and shared counter assertions."""

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric, Sum

from agent_framework_durabletask import _retention as retention
from agent_framework_durabletask import _retention_telemetry as telemetry

NOW = datetime(2026, 9, 11, 12, 0, 0, 123456, tzinfo=timezone.utc)
PREFIX = "durable.retention."


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    metric_reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[metric_reader], shutdown_on_exit=False)
    monkeypatch.setattr(telemetry, "get_meter", provider.get_meter)
    telemetry._instruments.cache_clear()
    clock = Mock(wraps=datetime)
    clock.now.return_value = NOW
    monkeypatch.setattr(retention, "datetime", clock)
    try:
        yield metric_reader
    finally:
        telemetry._instruments.cache_clear()
        provider.shutdown()


def _metrics(reader: InMemoryMetricReader) -> dict[str, Metric]:
    data = reader.get_metrics_data()
    if data is None:
        return {}
    result = {}
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            assert scope.scope.name == "agent_framework.durabletask"
            for metric in scope.metrics:
                assert metric.name.startswith(PREFIX)
                result[metric.name.removeprefix(PREFIX)] = metric
    return result


def _counter(metric: Metric, attributes: dict[str, Any], value: int) -> None:
    assert isinstance(metric.data, Sum)
    assert metric.data.is_monotonic
    matches = [point for point in metric.data.data_points if point.attributes == attributes]
    assert len(matches) == 1
    assert matches[0].value == value


def _counter_table(metric: Metric, expected: list[tuple[dict[str, Any], int]]) -> None:
    assert isinstance(metric.data, Sum)
    assert len(metric.data.data_points) == len(expected)
    for attributes, value in expected:
        _counter(metric, attributes, value)


def _attributes(mechanism: str = "pressure", outcome: str = "staged") -> dict[str, Any]:
    return {"mechanism": mechanism, "outcome": outcome, "commit_status": "not_attempted"}
