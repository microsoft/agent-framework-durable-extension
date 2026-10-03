# Durable Task tests

Test modules are named by the behavior they cover, not the review that introduced them.

## Areas

- `test_reader_*`, `test_response_*`, `test_shared_*` cover wire data and response projection.
- `test_delivery_*`, `test_history_*`, `test_compaction_*` cover delivery and history ownership.
- `test_entity_*`, `test_runtime_*`, `test_service_*` cover invocation and persistence boundaries.
- `test_workflow_*` cover registration, scheduling, state, HITL and SDK replay.
- `test_migration_*`, `test_state_migration_*`, `test_retention_*` cover migration and retention.
- `integration_tests/` contains service-backed tests. Constructed SDK histories in unit tests
  are not live service captures.

## Shared helpers

Reusable factories, SDK replay drivers and assertion helpers belong in non-collected
`_*test_support*.py` modules. Import helpers from those modules, not from `test_*.py` files.
Keep one-off helpers beside their tests. Different providers with different persistence or
ownership behavior should remain separate even when their names look similar.

The package-level pytest configuration registers support modules for assertion rewriting before
collection. When a test imports a shared pytest fixture, use an explicit re-export such as
`reader as reader` so fixture registration is intentional.

Tests that patch a shared helper's global dependency must patch its defining support module.
Keep serialized test types, counters and their restoration functions under one module owner.
Do not rewrite historical JSON captures to match a refactored harness.

## Running and reorganizing

Run pytest from the Python workspace or this package with the normal test dependencies installed.
Each package supplies its own test-owned deployment acknowledgement. Deployment-gate tests remove
or override it explicitly, and teardown restores the original environment.

After moving tests or helpers, compare parameterized collection before and after the move. Exercise
individual modules, each package separately, and reversed package collection. Preserve assertions,
fixture scopes and independent expected values rather than relying on an unchanged test count.