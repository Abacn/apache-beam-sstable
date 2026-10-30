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
# Build release wheels (macOS arm64 using current .venv, Linux x86_64 for
# Python 3.11..3.14 via build_linux_wheel.sh) and optionally tag & publish to
# GitHub Releases.
#
# Usage:
#   ./scripts/build_release_wheels.sh [--macos-only | --linux-only | --publish-only] [--publish] [--tag v0.1.0]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_BIN="${REPO_ROOT}/.venv/bin"
DELOCATE_BIN="${DELOCATE_BIN:-${VENV_BIN}/delocate-wheel}"

LINUX_PYTHON_VERSIONS=(3.11 3.12 3.13 3.14)
VERSION="${VERSION:-0.1.0}"
TAG="${TAG:-v${VERSION}}"
GITHUB_REMOTE="${GITHUB_REMOTE:-upstream-gh}"
GITHUB_REPO="${GITHUB_REPO:-Abacn/apache-beam-sstable}"

BUILD_MACOS=1
BUILD_LINUX=1
PUBLISH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --macos-only)
      BUILD_MACOS=1
      BUILD_LINUX=0
      shift
      ;;
    --linux-only)
      BUILD_MACOS=0
      BUILD_LINUX=1
      shift
      ;;
    --publish-only)
      BUILD_MACOS=0
      BUILD_LINUX=0
      PUBLISH=1
      shift
      ;;
    --publish)
      PUBLISH=1
      shift
      ;;
    --tag)
      TAG="$2"
      VERSION="${TAG#v}"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "${REPO_ROOT}/wheelhouse"

# ---------------------------------------------------------------------------
# 1. Build & verify macOS arm64 wheel using current .venv
# ---------------------------------------------------------------------------
if [[ "${BUILD_MACOS}" == "1" ]]; then
  echo "=================================================================="
  echo "Building macOS arm64 wheel using ${VENV_BIN}/python"
  echo "=================================================================="

  if [[ ! -x "${DELOCATE_BIN}" ]]; then
    "${VENV_BIN}/pip" install --quiet delocate build scikit-build-core pybind11
  fi

  MACOS_BUILD_DIR="${REPO_ROOT}/build/macos_arm64"
  MACOS_DIST_DIR="${REPO_ROOT}/build/dist_macos_arm64"
  rm -rf "${MACOS_BUILD_DIR}" "${MACOS_DIST_DIR}"
  mkdir -p "${MACOS_BUILD_DIR}" "${MACOS_DIST_DIR}"

  "${VENV_BIN}/python" -m build --wheel --no-isolation \
    -Cbuild-dir="${MACOS_BUILD_DIR}" \
    -Ccmake.define.CMAKE_DISABLE_FIND_PACKAGE_absl=ON \
    --outdir "${MACOS_DIST_DIR}" \
    "${REPO_ROOT}"

  rm -f "${REPO_ROOT}"/wheelhouse/apache_beam_sstable-"${VERSION}"-*macosx*arm64.whl
  "${DELOCATE_BIN}" -w "${REPO_ROOT}/wheelhouse" -v "${MACOS_DIST_DIR}"/*.whl

  MACOS_WHL="$(ls "${REPO_ROOT}"/wheelhouse/apache_beam_sstable-"${VERSION}"-*macosx*arm64.whl | head -n 1)"
  echo "--- [macOS arm64] Verifying ${MACOS_WHL} ---"
  "${VENV_BIN}/pip" install --quiet --force-reinstall --no-deps "${MACOS_WHL}"
  "${VENV_BIN}/python" -X faulthandler "${REPO_ROOT}/src/apache_beam_sstable/sstable_test.py" -v
  "${VENV_BIN}/python" -X faulthandler "${REPO_ROOT}/src/apache_beam_sstable/sstableio_test.py" -v
fi

# ---------------------------------------------------------------------------
# 2. Build & verify Linux x86_64 wheels (Python 3.11 - 3.14)
# ---------------------------------------------------------------------------
if [[ "${BUILD_LINUX}" == "1" ]]; then
  echo "=================================================================="
  echo "Building Linux x86_64 wheels for Python: ${LINUX_PYTHON_VERSIONS[*]}"
  echo "=================================================================="

  for py_ver in "${LINUX_PYTHON_VERSIONS[@]}"; do
    echo "--- [Linux x86_64] Running build_linux_wheel.sh for Python ${py_ver} ---"
    PYTHON_TAG="${py_ver}" bash "${REPO_ROOT}/scripts/build_linux_wheel.sh"
  done
fi

echo "=================================================================="
echo "Built wheels in ${REPO_ROOT}/wheelhouse:"
ls -lh "${REPO_ROOT}/wheelhouse"/apache_beam_sstable-"${VERSION}"-*.whl
echo "=================================================================="

# ---------------------------------------------------------------------------
# 3. Tag & publish to GitHub Releases
# ---------------------------------------------------------------------------
if [[ "${PUBLISH}" == "1" ]]; then
  echo "=================================================================="
  echo "Tagging ${TAG} and publishing to GitHub Releases (${GITHUB_REPO})"
  echo "=================================================================="

  if [[ -z "${GH_TOKEN:-}" && -z "${GITHUB_TOKEN:-}" ]]; then
    if ! gh auth status >/dev/null 2>&1; then
      CRED_TOKEN="$(printf "protocol=https\nhost=github.com\n\n" | git credential fill | sed -n 's/^password=//p')"
      if [[ -n "${CRED_TOKEN}" ]]; then
        export GH_TOKEN="${CRED_TOKEN}"
      fi
    fi
  fi

  if ! git rev-parse "${TAG}" >/dev/null 2>&1; then
    git tag -a "${TAG}" -m "Release ${TAG}"
  fi

  git push "${GITHUB_REMOTE}" HEAD
  git push "${GITHUB_REMOTE}" "${TAG}"

  WHEELS=("${REPO_ROOT}"/wheelhouse/apache_beam_sstable-"${VERSION}"-*.whl)

  if gh release view "${TAG}" --repo "${GITHUB_REPO}" >/dev/null 2>&1; then
    echo "Release ${TAG} already exists; uploading wheel assets..."
    gh release upload "${TAG}" "${WHEELS[@]}" --clobber --repo "${GITHUB_REPO}"
  else
    gh release create "${TAG}" "${WHEELS[@]}" \
      --repo "${GITHUB_REPO}" \
      --title "apache-beam-sstable ${TAG}" \
      --generate-notes
  fi

  echo "Published GitHub Release:"
  gh release view "${TAG}" --repo "${GITHUB_REPO}" --json url,assets
fi
