# Language-neutral schema validation cases

`validation-cases.json` records positive and negative review expectations using
the JSON Schema Test Suite's group shape: `description`, `schema`, and `tests`;
each test has `description`, `data`, and `valid`. It is test data, not a durable
state fixture, product implementation, or runtime test-project integration.

Resolve the canonical schema ID locally to `../durable-agent-entity-state.json`;
do not fetch it from GitHub, which might contain a different revision. Use a
Draft 2020-12 validator. The cases cover v2-only correlation constraints,
unchanged legacy/compaction handling, optional provisional binding, lossless
message shapes, structured-value presence, and historical ingestion scalars.

For example, from the repository root with an existing Python `jsonschema`
installation, this PowerShell command runs the structural cases without any
network access or dependency installation:

```powershell
@'
import json
from pathlib import Path
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

schema = json.loads(Path(r"schemas\durable-agent-entity-state.json").read_text(encoding="utf-8"))
Draft202012Validator.check_schema(schema)
registry = Registry().with_resource(schema["$id"], Resource.from_contents(schema))
groups = json.loads(Path(r"schemas\tests\validation-cases.json").read_text(encoding="utf-8"))
count = 0
for group in groups:
    validator = Draft202012Validator(group["schema"], registry=registry)
    for test in group["tests"]:
        actual = validator.is_valid(test["data"])
        if actual != test["valid"]:
            raise AssertionError(f'{group["description"]}: {test["description"]}')
        count += 1
print(f"Passed {count} structural validation cases")
'@ | python -
```

These cases have no timestamp-format assertions. Validate the separate state
fixtures with date-time format checking as well as the cross-map/time invariants
in the shared proposal. Test data alone cannot prove serializer round-tripping,
atomic entity commits, per-run ownership transitions, or runtime lookup behavior.
Future runtime implementations must additionally verify that original string
arguments, absent media types, opaque JSON metadata, and all present `value`
forms survive persistence without conversion, omission, or invented data.
