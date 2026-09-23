# Copyright (c) Microsoft. All rights reserved.

"""Registration-only doubles with the supported Durable Task SDK setup."""

from unittest.mock import Mock

from durabletask.serialization import JsonDataConverter
from durabletask.worker import TaskHubGrpcWorker


def create_registration_worker() -> Mock:
    """Record registration calls while exposing a real, stopped SDK converter."""
    worker = Mock(spec=TaskHubGrpcWorker)
    worker._data_converter = JsonDataConverter()
    worker._is_running = False
    return worker