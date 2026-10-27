#!/usr/bin/env python3
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

"""Vendors C++ SSTable and TSL files from TensorFlow v2.21.0 and applies patches."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import shutil
import ssl
import subprocess
import urllib.request

TF_TAG = "v2.21.0"
BASE_URL = f"https://raw.githubusercontent.com/tensorflow/tensorflow/{TF_TAG}"

# Patch files (relative to scripts/) applied in order after downloading upstream files.
PATCH_FILES: list[str] = [
    "tsl_platform_standalone.patch",
]

# Mapping of (upstream path relative to repo root) -> (local path relative to csrc/)
VENDORED_FILES: list[tuple[str, str]] = [
    # xla/tsl/lib/io (SSTable core)
    ("third_party/xla/xla/tsl/lib/io/block.cc", "xla/tsl/lib/io/block.cc"),
    ("third_party/xla/xla/tsl/lib/io/block.h", "xla/tsl/lib/io/block.h"),
    ("third_party/xla/xla/tsl/lib/io/block_builder.cc", "xla/tsl/lib/io/block_builder.cc"),
    ("third_party/xla/xla/tsl/lib/io/block_builder.h", "xla/tsl/lib/io/block_builder.h"),
    ("third_party/xla/xla/tsl/lib/io/cache.cc", "xla/tsl/lib/io/cache.cc"),
    ("third_party/xla/xla/tsl/lib/io/cache.h", "xla/tsl/lib/io/cache.h"),
    ("third_party/xla/xla/tsl/lib/io/format.cc", "xla/tsl/lib/io/format.cc"),
    ("third_party/xla/xla/tsl/lib/io/format.h", "xla/tsl/lib/io/format.h"),
    ("third_party/xla/xla/tsl/lib/io/iterator.cc", "xla/tsl/lib/io/iterator.cc"),
    ("third_party/xla/xla/tsl/lib/io/iterator.h", "xla/tsl/lib/io/iterator.h"),
    ("third_party/xla/xla/tsl/lib/io/table.cc", "xla/tsl/lib/io/table.cc"),
    ("third_party/xla/xla/tsl/lib/io/table.h", "xla/tsl/lib/io/table.h"),
    ("third_party/xla/xla/tsl/lib/io/table_builder.cc", "xla/tsl/lib/io/table_builder.cc"),
    ("third_party/xla/xla/tsl/lib/io/table_builder.h", "xla/tsl/lib/io/table_builder.h"),
    ("third_party/xla/xla/tsl/lib/io/table_format.txt", "xla/tsl/lib/io/table_format.txt"),
    ("third_party/xla/xla/tsl/lib/io/table_options.h", "xla/tsl/lib/io/table_options.h"),
    (
        "third_party/xla/xla/tsl/lib/io/two_level_iterator.cc",
        "xla/tsl/lib/io/two_level_iterator.cc",
    ),
    (
        "third_party/xla/xla/tsl/lib/io/two_level_iterator.h",
        "xla/tsl/lib/io/two_level_iterator.h",
    ),
    # xla/tsl/lib/hash (CRC32C)
    ("third_party/xla/xla/tsl/lib/hash/crc32c.cc", "xla/tsl/lib/hash/crc32c.cc"),
    ("third_party/xla/xla/tsl/lib/hash/crc32c.h", "xla/tsl/lib/hash/crc32c.h"),
    # xla/tsl/platform headers
    ("third_party/xla/xla/tsl/platform/byte_order.h", "xla/tsl/platform/byte_order.h"),
    (
        "third_party/xla/xla/tsl/platform/default/status.h",
        "xla/tsl/platform/default/status.h",
    ),
    ("third_party/xla/xla/tsl/platform/env.h", "xla/tsl/platform/env.h"),
    ("third_party/xla/xla/tsl/platform/env_time.h", "xla/tsl/platform/env_time.h"),
    ("third_party/xla/xla/tsl/platform/errors.h", "xla/tsl/platform/errors.h"),
    (
        "third_party/xla/xla/tsl/platform/file_statistics.h",
        "xla/tsl/platform/file_statistics.h",
    ),
    ("third_party/xla/xla/tsl/platform/file_system.h", "xla/tsl/platform/file_system.h"),
    ("third_party/xla/xla/tsl/platform/logging.h", "xla/tsl/platform/logging.h"),
    ("third_party/xla/xla/tsl/platform/macros.h", "xla/tsl/platform/macros.h"),
    (
        "third_party/xla/xla/tsl/platform/stack_frame.h",
        "xla/tsl/platform/stack_frame.h",
    ),
    ("third_party/xla/xla/tsl/platform/status.h", "xla/tsl/platform/status.h"),
    ("third_party/xla/xla/tsl/platform/types.h", "xla/tsl/platform/types.h"),
    # tsl/platform headers & coding
    ("third_party/xla/third_party/tsl/tsl/platform/coding.cc", "tsl/platform/coding.cc"),
    ("third_party/xla/third_party/tsl/tsl/platform/coding.h", "tsl/platform/coding.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/cord.h", "tsl/platform/cord.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/ctstring.h", "tsl/platform/ctstring.h"),
    (
        "third_party/xla/third_party/tsl/tsl/platform/ctstring_internal.h",
        "tsl/platform/ctstring_internal.h",
    ),
    ("third_party/xla/third_party/tsl/tsl/platform/mutex.h", "tsl/platform/mutex.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/numa.h", "tsl/platform/numa.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/numbers.h", "tsl/platform/numbers.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/platform.h", "tsl/platform/platform.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/raw_coding.h", "tsl/platform/raw_coding.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/refcount.h", "tsl/platform/refcount.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/snappy.h", "tsl/platform/snappy.h"),
    ("third_party/xla/third_party/tsl/tsl/platform/strcat.h", "tsl/platform/strcat.h"),
    (
        "third_party/xla/third_party/tsl/tsl/platform/stringpiece.h",
        "tsl/platform/stringpiece.h",
    ),
    (
        "third_party/xla/third_party/tsl/tsl/platform/thread_annotations.h",
        "tsl/platform/thread_annotations.h",
    ),
    ("third_party/xla/third_party/tsl/tsl/platform/tstring.h", "tsl/platform/tstring.h"),
]


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      "--no-patch",
      action="store_true",
      help="Only vendor unmodified upstream files without applying scripts/*.patch.",
  )
  args = parser.parse_args()

  scripts_dir = Path(__file__).resolve().parent
  repo_root = scripts_dir.parent
  csrc_dir = repo_root / "csrc"

  for subdir in ("xla", "tsl"):
    target_sub = csrc_dir / subdir
    if target_sub.exists():
      shutil.rmtree(target_sub)

  cafile = "/etc/ssl/cert.pem" if Path("/etc/ssl/cert.pem").exists() else None
  ssl_ctx = ssl.create_default_context(cafile=cafile)

  print(f"Vendoring {len(VENDORED_FILES)} unmodified files from tensorflow {TF_TAG}...")
  for upstream_rel, local_rel in VENDORED_FILES:
    url = f"{BASE_URL}/{upstream_rel}"
    dest = csrc_dir / local_rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, context=ssl_ctx) as response:
      data = response.read()
    dest.write_bytes(data)
    sha256 = hashlib.sha256(data).hexdigest()[:12]
    print(f"  [OK] {local_rel} ({len(data)} bytes, sha256={sha256})")

  if not args.no_patch:
    for patch_name in PATCH_FILES:
      patch_path = scripts_dir / patch_name
      print(f"Applying patch {patch_name}...")
      subprocess.run(
          ["patch", "-p1", "-i", str(patch_path)],
          cwd=str(repo_root),
          check=True,
      )
      print(f"  [PATCHED] {patch_name}")

  print("Done.")


if __name__ == "__main__":
  main()
