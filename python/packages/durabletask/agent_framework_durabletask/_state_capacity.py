# Copyright (c) Microsoft. All rights reserved.

"""State-capacity exceptions shared by detached migration and future budget checks."""

from __future__ import annotations

__all__ = ["StateCapacityError"]


class StateCapacityError(ValueError):
    """The protected state or an unreachable capacity target prevents a safe commit."""

    def __init__(self, *, size_bytes: int, max_state_bytes: int, floor_bytes: int, target_bytes: int) -> None:
        """Describe the measured state, configured budget, protected floor and target."""
        self.size_bytes = size_bytes
        self.max_state_bytes = max_state_bytes
        self.floor_bytes = floor_bytes
        self.target_bytes = target_bytes
        super().__init__(
            f"Durable state capacity cannot meet the {target_bytes}-byte capacity target: "
            f"serialized size is {size_bytes} bytes, budget is {max_state_bytes} bytes, "
            f"and the protected floor is {floor_bytes} bytes. No transcript changes were applied."
        )
