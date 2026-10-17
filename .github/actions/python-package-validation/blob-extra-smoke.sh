#!/usr/bin/env bash
# Copyright (c) Microsoft. All rights reserved.

set -eo pipefail

smoke_root="$(mktemp -d "$RUNNER_TEMP/blob-extra.XXXXXX")"
trap 'rm -rf "$smoke_root"' EXIT
uv venv --python "$PYTHON_VERSION" "$smoke_root/venv"
smoke_python="$smoke_root/venv/bin/python"
if [[ ! -f "$smoke_python" ]]; then smoke_python="$smoke_root/venv/Scripts/python.exe"; fi
wheels=(dist/agent_framework_"$PACKAGE"-*.whl)
[[ ${#wheels[@]} == 1 && -f "${wheels[0]}" ]]
requirements=("${wheels[0]}[azure-blob-payloads]" packaging)
if [[ "$PACKAGE" == azurefunctions ]]; then
  uv build "$GITHUB_WORKSPACE/python/packages/durabletask" --wheel --out-dir "$smoke_root/wheels"
  durabletask_wheels=("$smoke_root"/wheels/agent_framework_durabletask-*.whl)
  [[ ${#durabletask_wheels[@]} == 1 && -f "${durabletask_wheels[0]}" ]]
  requirements+=("${durabletask_wheels[0]}")
fi
uv pip install --python "$smoke_python" --prerelease if-necessary-or-explicit "${requirements[@]}"
uv pip check --python "$smoke_python"
cd "$smoke_root"
"$smoke_python" -I - <<'PY'
import importlib
import json
import os
import sys
from importlib import metadata
from pathlib import Path
from urllib.parse import urlparse

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

extra = "azure-blob-payloads"
package = os.environ["PACKAGE"]
expected_python = tuple(map(int, os.environ["PYTHON_VERSION"].split(".")))
assert sys.version_info[:2] == expected_python
distributions = ["agent-framework-durabletask"]
if package == "azurefunctions":
    distributions.append("agent-framework-azurefunctions")
for name in distributions:
    distribution = metadata.distribution(name)
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    assert urlparse(direct_url.get("url", "")).path.endswith(".whl"), name
    assert not direct_url.get("dir_info", {}).get("editable"), name
    module = importlib.import_module(name.replace("-", "_"))
    assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), name
    assert extra in distribution.metadata.get_all("Provides-Extra", [])
    target = "durabletask" if name == "agent-framework-durabletask" else "agent-framework-durabletask"
    requirements = [Requirement(value) for value in distribution.requires or []]
    assert any(
        canonicalize_name(requirement.name) == target
        and extra in requirement.extras
        and requirement.marker is not None
        and requirement.marker.evaluate({"extra": extra})
        and not requirement.marker.evaluate({"extra": ""})
        for requirement in requirements
    ), f"{name} does not forward its Blob extra"
for name in ("azure-storage-blob", "aiohttp"):
    print(name, metadata.version(name))
from azure.storage.blob.aio import BlobServiceClient
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

assert BlobServiceClient and BlobPayloadStore and BlobPayloadStoreOptions
print(f"{package} built-wheel Blob extra smoke passed on Python {sys.version.split()[0]}")
PY
