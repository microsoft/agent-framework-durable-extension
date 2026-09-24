# Azure Functions tests

The suite separates Functions-specific registration, HTTP and SDK behavior from the shared
Durable Task engine. `integration_tests/` runs real Functions hosts and backing services.
Unit tests that construct SDK event histories are not live integration tests.

Shared Functions helpers live in non-collected `_*test_support*.py` modules here. Helpers for
the common runtime live in the sibling [Durable Task test suite](../../durabletask/tests/README.md).
The pytest configuration declares that helper path, so Functions tests do not depend on the
Durable Task tests being collected first.

Import shared fixtures explicitly, for example `app as app`, and keep one-off helpers local.
Do not import helpers from collected `test_*.py` modules. The package-level configuration
registers both packages' support modules for assertion rewriting before collection.

When moving a helper, preserve its SDK version behavior, mutable counters, monkeypatch targets
and fixture teardown. Validate Functions-only execution as well as the combined suite. Keep
historical captures and independent response expectations unchanged.