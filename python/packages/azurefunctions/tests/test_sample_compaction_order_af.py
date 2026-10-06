# Copyright (c) Microsoft. All rights reserved.

"""Exercise the real Functions compaction sample factory offline."""

import pytest
from test_sample_compaction_order import _assert_fourth_call_observes_four_prior_groups


@pytest.mark.parametrize("history_first", [False, True], ids=["actual-sample", "old-order-control"])
async def test_functions_sample_fourth_model_call_observes_four_prior_groups_after_cold_reloads(
    history_first: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _assert_fourth_call_observes_four_prior_groups("functions", history_first, monkeypatch)
