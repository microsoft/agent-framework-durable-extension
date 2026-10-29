# Copyright (c) Microsoft. All rights reserved.

"""Shared typing for checkpoint data that crosses JSON and framework boundaries."""

from __future__ import annotations

import math
from typing import Any, TypeAlias, TypedDict, TypeGuard, cast

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, Any]
StringKeyedDict: TypeAlias = dict[str, Any]
DynamicList: TypeAlias = list[Any]


class _OperationAttemptRequired(TypedDict):
    nativeOperationId: str
    originalCorrelation: str
    requestDigest: str
    runtimeCorrelation: str
    attempt: int


class OperationAttempt(_OperationAttemptRequired, total=False):
    inputOccurrences: list[dict[str, str]]


def is_string_keyed_dict(value: Any) -> TypeGuard[StringKeyedDict]:
    """Narrow a dynamic mapping to string keys without constraining its values."""
    if not isinstance(value, dict):
        return False
    dynamic_dict = cast(dict[object, Any], value)
    return all(isinstance(key, str) for key in dynamic_dict)


def is_list(value: Any) -> TypeGuard[DynamicList]:
    """Narrow a dynamic value to a list without constraining its elements."""
    return isinstance(value, list)


def is_json_value(value: object) -> TypeGuard[JsonValue]:
    """Check that a value contains only recursively valid JSON data."""
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if is_list(value):
        return all(is_json_value(item) for item in value)
    if is_string_keyed_dict(value):
        return all(is_json_value(item) for item in value.values())
    return False


def is_json_object(value: Any) -> TypeGuard[JsonObject]:
    """Narrow a dynamic value to a recursively valid JSON object."""
    return is_string_keyed_dict(value) and all(is_json_value(item) for item in value.values())


def is_json_array(value: Any) -> TypeGuard[DynamicList]:
    """Narrow a dynamic value to a recursively valid JSON array."""
    return is_list(value) and all(is_json_value(item) for item in value)


def is_list_or_tuple(value: Any) -> TypeGuard[list[Any] | tuple[Any, ...]]:
    """Narrow row-like checkpoint data while retaining legacy tuple support."""
    return isinstance(value, (list, tuple))
