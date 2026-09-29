# Copyright (c) Microsoft. All rights reserved.

"""Keep shared test-helper assertions equivalent to assertions in collected tests."""

from pathlib import Path

import pytest

for support in Path(__file__).parent.parent.glob("*/tests/_*test_support*.py"):
    pytest.register_assert_rewrite(support.stem)
