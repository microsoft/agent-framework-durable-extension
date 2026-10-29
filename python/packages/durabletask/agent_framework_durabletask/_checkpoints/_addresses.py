# Copyright (c) Microsoft. All rights reserved.

"""Published release child-address policy for semantic continuation."""


def published_subworkflow_instance_id(parent_instance_id: str, executor_id: str, ordinal: int) -> str:
    if not isinstance(parent_instance_id, str) or not parent_instance_id:
        raise ValueError("Parent instance ID must be a nonempty string.")
    if not isinstance(executor_id, str) or not executor_id:
        raise ValueError("Executor ID must be a nonempty string.")
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("Child ordinal must be a nonnegative integer.")
    return f"{parent_instance_id}::{executor_id}::{ordinal}"
