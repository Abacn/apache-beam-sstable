#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_TAG="${PYTHON_TAG:-3.11}"
PY_ABI="cp${PYTHON_TAG//./}"
BEAM_VERSION="${BEAM_VERSION:-2.77.0}"
BUILD_IMAGE="docker.io/library/python:${PYTHON_TAG}-bookworm"
BEAM_SDK_IMAGE="docker.io/apache/beam_python${PYTHON_TAG}_sdk:${BEAM_VERSION}"
CONTAINER_ENGINE="${CONTAINER_ENGINE:-/opt/homebrew/bin/podman}"

mkdir -p "${REPO_ROOT}/wheelhouse"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "1. Building linux/x86_64 wheel using ${BUILD_IMAGE} via ${CONTAINER_ENGINE}..."

  "${CONTAINER_ENGINE}" run --rm \
    --platform linux/amd64 \
    -e PY_ABI="${PY_ABI}" \
    -v "${REPO_ROOT}:/workspace" \
    -w /workspace \
    "${BUILD_IMAGE}" \
    bash -c '
      set -euo pipefail
      export DEBIAN_FRONTEND=noninteractive
      export CMAKE_BUILD_PARALLEL_LEVEL=2
      apt-get update -qq
      apt-get install -y -qq --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        libsnappy-dev

      python -m pip install --quiet --upgrade pip build scikit-build-core pybind11 auditwheel patchelf

      rm -rf /tmp/dist /tmp/wheelhouse
      mkdir -p /tmp/dist /tmp/wheelhouse /workspace/build/linux_x86_64

      python -m build --wheel --no-isolation -Cbuild-dir=/workspace/build/linux_x86_64 --outdir /tmp/dist /workspace
      auditwheel repair /tmp/dist/*.whl -w /tmp/wheelhouse
      rm -f /workspace/wheelhouse/apache_beam_sstable-*-"${PY_ABI}"-"${PY_ABI}"-*linux*.whl
      cp /tmp/wheelhouse/*.whl /workspace/wheelhouse/
      ls -lh /workspace/wheelhouse/*.whl
    '
fi

echo "2. Verifying wheel installation and tests inside ${BEAM_SDK_IMAGE}..."

"${CONTAINER_ENGINE}" run --rm \
  --platform linux/amd64 \
  -e PY_ABI="${PY_ABI}" \
  -v "${REPO_ROOT}:/workspace" \
  -w /tmp \
  --entrypoint /bin/bash \
  "${BEAM_SDK_IMAGE}" \
  -c '
    set -euo pipefail
    pip install --no-deps /workspace/wheelhouse/apache_beam_sstable-*-"${PY_ABI}"-"${PY_ABI}"-*manylinux*_x86_64.whl
    echo "--- Testing C++ extension module (sstable_test.py) ---"
    python -X faulthandler /workspace/src/apache_beam_sstable/sstable_test.py -v
    echo "--- Testing Beam PTransforms (sstableio_test.py) ---"
    # Note: pyarrow on x86_64 crashes on dlopen under QEMU user-mode emulation on Apple Silicon.
    if [ "$(uname -r | grep -c aarch64)" -gt 0 ]; then
      pip uninstall -y -q pyarrow
    fi
    python -X faulthandler /workspace/src/apache_beam_sstable/sstableio_test.py -v
  '
