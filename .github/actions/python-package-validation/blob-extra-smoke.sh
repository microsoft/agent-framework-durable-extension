#!/usr/bin/env bash
# Copyright (c) Microsoft. All rights reserved.

set -eo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
"$smoke_python" -I "$script_dir/blob-extra-smoke.py"
