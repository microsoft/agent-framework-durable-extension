# Copyright (c) Microsoft. All rights reserved.

"""Datetime test doubles that retain the real datetime instance contract."""

from datetime import datetime as OriginalDateTime


class _DateTimeType(type):
    def __instancecheck__(cls, instance: object) -> bool:
        # Explicit now values and parsed timestamps need not be clock subclasses.
        return isinstance(instance, OriginalDateTime)


class ClockDateTime(OriginalDateTime, metaclass=_DateTimeType):
    """Base for fixture-local clocks without patching the standard library."""
