# Language-neutral schema validation cases

`validation-cases.json` and `versioned-envelope-cases.json` record positive and negative review expectations using
the JSON Schema Test Suite's group shape: `description`, `schema`, and `tests`;
each test has `description`, `data`, and `valid`. These are test data, not durable
state fixtures, product implementation, or runtime test-project integration.

Resolve the canonical schema ID locally to `../durable-agent-entity-state.json`;
do not fetch it from GitHub, which might contain a different revision. Use a
Draft 2020-12 validator. The cases cover v2-only correlation constraints,
unchanged legacy/compaction handling, opaque runtime profiles, v2 lossless
message shapes, structured-value presence, and historical ingestion scalars.

The versioned cases use complete root envelopes for every version rather than
just testing `$defs` fragments. Historical `1.0.0`, `1.1.0`, and `1.2.0` reject
the newly widened developer role, string-form function arguments, and URI content
without media type; v2 accepts them in its transcript and terminal payload paths.
Historical explicit `unknown` JSON was already valid and stays valid in all
versions. The unchanged legacy fixture remains additional compatibility evidence.
Profile cases distinguish opaque shared preservation from validation by a relying
runtime. Outcome cases reject attempts to promote receipts lacking authoritative
outcome into the required-outcome v2 shape; they do not implement migration.

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
count = 0
for path in sorted(Path(r"schemas\tests").glob("*-cases.json")):
    groups = json.loads(path.read_text(encoding="utf-8"))
    for group in groups:
        validator = Draft202012Validator(group["schema"], registry=registry)
        for test in group["tests"]:
            actual = validator.is_valid(test["data"])
            if actual != test["valid"]:
                raise AssertionError(f'{path.name}: {group["description"]}: {test["description"]}')
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
