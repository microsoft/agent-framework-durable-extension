# Copyright (c) Microsoft. All rights reserved.

"""Unit tests for selector validation and synchronous edge-condition evaluation.

Durable orchestrators run as generators and evaluate edge conditions
synchronously. A condition that returns an awaitable cannot be evaluated in
that context, so the edge is treated as *not matched* (not traversed).
"""

from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework._workflows._edge import (
    Edge,
    FanOutEdgeGroup,
    SingleEdgeGroup,
    SwitchCaseEdgeGroup,
    SwitchCaseEdgeGroupCase,
    SwitchCaseEdgeGroupDefault,
)
from agent_framework._workflows._edge_runner import FanOutEdgeRunner
from agent_framework._workflows._runner_context import WorkflowMessage

from agent_framework_durabletask._workflows.orchestrator import (
    _evaluate_edge_condition_sync,
    route_message_through_edge_groups,
)


@pytest.fixture(params=[FanOutEdgeGroup, SwitchCaseEdgeGroup])
def selectable_group(request: pytest.FixtureRequest) -> FanOutEdgeGroup:
    if request.param is SwitchCaseEdgeGroup:
        return SwitchCaseEdgeGroup(
            source_id="source",
            cases=[
                SwitchCaseEdgeGroupCase(condition=lambda m: True, target_id="target_a"),
                SwitchCaseEdgeGroupDefault(target_id="target_b"),
            ],
        )
    return FanOutEdgeGroup(source_id="source", target_ids=["target_a", "target_b"])


class TestSelectorTargetValidation:
    @pytest.mark.parametrize(
        "selected",
        [
            ["other_target"],
            ["target_a", "other_target"],
            ["other_target", "target_a"],
            ["missing"],
            [""],
            [None],
            [["target_a"]],
        ],
    )
    async def test_invalid_selection_matches_core(self, selectable_group: FanOutEdgeGroup, selected: list[Any]) -> None:
        selector = Mock(return_value=selected)
        selectable_group._selection_func = selector
        # The off-group target can be valid on a different edge. That does not
        # make it a valid selection for this group.
        other_group = SingleEdgeGroup(source_id="source", target_id="other_target")
        core_runner = FanOutEdgeRunner(selectable_group, {})
        message = WorkflowMessage(data={"targets": selected}, source_id="source")

        with pytest.raises(RuntimeError, match="Invalid selection result") as core_error:
            await core_runner.send_message(message, Mock(), Mock())

        selector.reset_mock()
        with pytest.raises(RuntimeError, match="Invalid selection result") as durable_error:
            route_message_through_edge_groups([other_group, selectable_group], "source", message.data)

        assert str(durable_error.value) == str(core_error.value)
        selector.assert_called_once_with(message.data, ["target_a", "target_b"])

    @pytest.mark.parametrize(
        "selected", [[], ["target_a"], ["target_b"], ["target_b", "target_a"], ["target_a", "target_a"]]
    )
    def test_valid_selection_preserves_order_and_duplicates(
        self, selectable_group: FanOutEdgeGroup, selected: list[str]
    ) -> None:
        selector = Mock(return_value=selected)
        selectable_group._selection_func = selector

        assert route_message_through_edge_groups([selectable_group], "source", "payload") == selected
        selector.assert_called_once_with("payload", ["target_a", "target_b"])

    def test_selector_cannot_expand_targets_by_mutating_argument(self, selectable_group: FanOutEdgeGroup) -> None:
        def select(message: Any, targets: list[str]) -> list[str]:
            targets.append(message)
            return targets

        selectable_group._selection_func = select

        with pytest.raises(RuntimeError, match="Invalid selection result"):
            route_message_through_edge_groups([selectable_group], "source", "other_target")

        assert selectable_group.target_executor_ids == ["target_a", "target_b"]

    def test_without_selector_broadcasts(self, selectable_group: FanOutEdgeGroup) -> None:
        selectable_group._selection_func = None

        assert route_message_through_edge_groups([selectable_group], "source", "payload") == ["target_a", "target_b"]

    def test_other_source_does_not_invoke_selector(self, selectable_group: FanOutEdgeGroup) -> None:
        selector = Mock(side_effect=AssertionError("Selector must not run for another source"))
        selectable_group._selection_func = selector

        assert route_message_through_edge_groups([selectable_group], "other_source", "payload") == []
        selector.assert_not_called()


class TestEvaluateEdgeConditionSync:
    """Synchronous edge-condition evaluation semantics."""

    def test_no_condition_traverses(self) -> None:
        edge = Edge("a", "b")
        assert _evaluate_edge_condition_sync(edge, {"x": 1}) is True

    def test_sync_true_traverses(self) -> None:
        edge = Edge("a", "b", condition=lambda m: m["ok"])
        assert _evaluate_edge_condition_sync(edge, {"ok": True}) is True

    def test_sync_false_does_not_traverse(self) -> None:
        edge = Edge("a", "b", condition=lambda m: m["ok"])
        assert _evaluate_edge_condition_sync(edge, {"ok": False}) is False

    def test_async_condition_is_not_traversed(self) -> None:
        # The durabletask host evaluates conditions synchronously; an async
        # condition cannot be evaluated, so the edge is treated as not matched
        # even though it would resolve True when awaited.
        async def gate(_message: object) -> bool:
            return True

        edge = Edge("a", "b", condition=gate)
        assert _evaluate_edge_condition_sync(edge, {"x": 1}) is False
