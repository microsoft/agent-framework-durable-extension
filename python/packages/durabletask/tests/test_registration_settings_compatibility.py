# Copyright (c) Microsoft. All rights reserved.

"""Exercise the exported settings constructor, not only host keyword callers."""

import inspect
from dataclasses import FrozenInstanceError, dataclass, replace
from typing import Any

import pytest

from agent_framework_durabletask import AgentRegistrationSettings


@pytest.mark.parametrize("style", ["positional", "keyword", "mixed"])
@pytest.mark.parametrize("with_callback", [False, True])
def test_legacy_constructor_preserves_window_and_callback(style: str, with_callback: bool) -> None:
    callback: Any = object() if with_callback else None
    if style == "positional":
        settings = AgentRegistrationSettings(17, callback) if with_callback else AgentRegistrationSettings(17)
    elif style == "keyword":
        settings = AgentRegistrationSettings(response_delivery_window_seconds=17, callback=callback)
    else:
        settings = AgentRegistrationSettings(17, callback=callback)

    assert settings.response_delivery_window_seconds == 17
    assert settings.callback is callback
    assert settings.matches(AgentRegistrationSettings(17, callback))
    assert not settings.matches(AgentRegistrationSettings(18, callback))


def test_legacy_callback_comparison_and_frozen_contract() -> None:
    class EqualCallback:
        def __eq__(self, other: object) -> bool:
            return isinstance(other, EqualCallback)

    first: Any = EqualCallback()
    second: Any = EqualCallback()
    left = AgentRegistrationSettings(17, first)
    right = AgentRegistrationSettings(17, second)
    assert first == second and first is not second
    assert left == right and hash(left) == hash(right)
    assert not left.matches(right)
    assert left.matches(replace(left))
    writable: Any = left
    with pytest.raises(FrozenInstanceError):
        writable.response_delivery_window_seconds = 19


def test_legacy_signature_retains_required_window_and_first_two_positional_fields() -> None:
    signature = inspect.signature(AgentRegistrationSettings)
    parameters = list(signature.parameters.values())
    assert [parameter.name for parameter in parameters[:2]] == ["response_delivery_window_seconds", "callback"]
    assert all(parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD for parameter in parameters[:2])
    assert parameters[0].default is inspect.Parameter.empty
    assert parameters[1].default is None
    with pytest.raises(TypeError):
        signature.bind()


def test_legacy_subclass_positional_field_and_pattern_keep_their_original_meaning() -> None:
    @dataclass(frozen=True)
    class NamedSettings(AgentRegistrationSettings):
        label: str = "default"

    callback: Any = object()
    settings = NamedSettings(17, callback, "application-label")
    assert settings.label == "application-label"
    assert settings.response_delivery_window_seconds == 17 and settings.callback is callback
    match settings:
        case NamedSettings(window, registered_callback, label):
            assert window == 17 and registered_callback is callback and label == "application-label"
        case _:
            pytest.fail("Legacy positional pattern did not match")
    assert AgentRegistrationSettings.__match_args__ == ("response_delivery_window_seconds", "callback")


def test_old_constructor_uses_non_evicting_retention_defaults() -> None:
    settings = AgentRegistrationSettings(17)
    assert settings.retention == "keep_all"
    assert settings.max_state_bytes is None
    assert settings.high_watermark == 0.85
    assert settings.low_watermark == 0.70


@pytest.mark.parametrize(
    ("field", "value"),
    [("retention", "follow_compaction"), ("max_state_bytes", 8192), ("high_watermark", 0.99), ("low_watermark", 0.1)],
)
def test_new_settings_still_participate_in_registration_identity(field: str, value: Any) -> None:
    callback: Any = object()
    original = AgentRegistrationSettings(17, callback)
    changed = replace(original, **{field: value})
    assert changed != original and not original.matches(changed)
    assert changed.callback is callback and changed.response_delivery_window_seconds == 17
    assert changed.matches(replace(changed))


def test_all_keyword_constructor_preserves_explicit_retention_settings() -> None:
    callback: Any = object()
    settings = AgentRegistrationSettings(
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.4,
        response_delivery_window_seconds=17,
        callback=callback,
    )
    assert settings.response_delivery_window_seconds == 17 and settings.callback is callback
    assert settings.retention == "follow_compaction" and settings.max_state_bytes == 8192
    assert settings.high_watermark == 0.95 and settings.low_watermark == 0.4
