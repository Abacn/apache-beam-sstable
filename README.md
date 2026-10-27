<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Python SSTable IO (`apache-beam-sstable`)

High-performance Apache Beam Python SDK connector (`ReadFromSSTable`, `ReadAllFromSSTable`, `WriteToSSTable`, `LookupInSSTable`) and low-level Python bindings (`SSTableReader`, `SSTableBuilder`, `SortingSSTableBuilder`) backed by the C++ `tsl::table` SSTable implementation from TensorFlow (`xla/tsl/lib/io`).

## Features

- **Exact Wire-Format Compatibility**: Uses TensorFlow `v2.21.0`'s `xla/tsl/lib/io` SSTable implementation (`magic = 0xdb4775248b80fb57`, prefix-compressed data & index blocks, CRC32C block trailers, Snappy compression, sharded LRU block cache).
- **Splittable `FileBasedSource` Read Path (`ReadFromSSTable`, `ReadAllFromSSTable`)**:
  - Reads only the 48-byte Footer and Index Block at split time (`InspectTableBlocks`) to compute exact SSTable block boundaries.
  - Supports initial splitting and dynamic work rebalancing (`OffsetRangeTracker`) with zero duplicate or skipped records across sub-file byte splits.
  - Supports `key_prefix`, `[start_key, end_key)` range scans, and `keys_only=True`.
- **Resilient, Dynamically-Sharded, Externally-Sorted Write Path (`WriteToSSTable`)**:
  - Built on Apache Beam's `FileBasedSink` (`_SSTableSink`) with runner-determined bundle sharding (`num_shards=0`), `max_records_per_shard` / `max_bytes_per_shard` rollover, two-phase temporary directory staging, and multi-threaded batched finalization.
  - Out-of-core C++ `SortingSSTableBuilder` that buffers and sorts records in native C++ memory (`py::gil_scoped_release`) up to `max_sorting_memory_mb`, spills sorted intermediate runs to local temporary SSTables if needed, and performs a $K$-way min-heap merge directly into Beam's `FileSystems.create()` buffered channel.

---

## System Build Requirements

The C++ extension (`_sstable_cc`) is built using CMake (`>= 3.24`) and `scikit-build-core` with a C++17 compiler. It links against three system C++ libraries (without requiring `libprotobuf`, `Eigen`, or `ml_dtypes`):

1. **CMake** (`>= 3.24`) and **Ninja** (recommended build generator)
2. **Abseil C++ (`abseil-cpp >= 20250814`)** (`absl::base`, `absl::cord`, `absl::crc32c`, `absl::log`, `absl::check`, `absl::status`, `absl::strings`, `absl::synchronization`, `absl::span`; fetched automatically via CMake `FetchContent` if not installed on the system)
3. **Snappy** (`libsnappy` header and library)
4. **pybind11** (`>= 2.11`)

### Installing Build Dependencies on macOS (`arm64`)

Install the required native build tools and libraries via Homebrew:

```bash
brew install cmake ninja abseil snappy pybind11
```

### Installing Build Dependencies on Linux (`x86_64`)

#### Debian / Ubuntu

Install `cmake`, `ninja-build`, and `libsnappy-dev` via `apt` (`CMakeLists.txt` automatically fetches and links `abseil-cpp 20250814.1` when system `abseil-cpp` is not installed):

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  cmake \
  ninja-build \
  libsnappy-dev \
  patchelf \
  python3-dev \
  python3-venv
```

---

## Local Development & Testing (Virtualenv)

Always use an isolated Python virtual environment for development and testing:

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install editable package and test dependencies
pip install --upgrade pip
pip install -e ".[test]"

# 3. Run the unit and integration test suites
pytest src/apache_beam_sstable/
```

To build the C++ extension directly with CMake + Ninja during C++ iteration:

```bash
cmake -B build -G Ninja
cmake --build build
```

---

## Building Distributable Python Wheels (`macOS/arm64` and `Linux/x86_64`)

When deploying to Cloud Dataflow (`--extra_package`) or distributing pre-compiled wheels, native shared library dependencies (`libsnappy`) should be bundled into the wheel so worker VMs do not need system packages pre-installed.

### 1. Building & Verifying `linux/x86_64` Wheels for Cloud Dataflow (`scripts/build_linux_wheel.sh`)

Use [`scripts/build_linux_wheel.sh`](scripts/build_linux_wheel.sh) to build a `manylinux_2_34_x86_64` wheel using `python:3.11-bookworm` (`glibc 2.36`, matching Cloud Dataflow's Debian 12 Bookworm workers), repair it with `auditwheel`, and verify installation and unit tests inside `apache/beam_python3.11_sdk:2.77.0`:

```bash
./scripts/build_linux_wheel.sh
```

The script outputs the self-contained wheel to `wheelhouse/apache_beam_sstable-0.1.0-cp311-cp311-manylinux_2_34_x86_64.whl` and runs `sstable_test.py` and `sstableio_test.py` inside the official Apache Beam Python 3.11 SDK container image.

### 2. Building Wheels on macOS (`arm64` / `macosx_11_0_arm64`)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip build delocate

# Build the raw wheel
python -m build --wheel --outdir dist/

# Bundle dynamic libraries (.dylib) into the wheel using delocate
delocate-wheel -w wheelhouse/ -v dist/*.whl
```

---

## Running the Cloud Dataflow Load Test (`dataflow_load_test.py`)

[`src/apache_beam_sstable/dataflow_load_test.py`](src/apache_beam_sstable/dataflow_load_test.py) runs two separate Beam pipelines:
1. **Write pipeline (`--mode=write` or `--mode=all`)**: Generates synthetic `(key, value)` records using Beam's `SyntheticSource(algorithm="lcg")` and writes dynamically sharded SSTables via `WriteToSSTable`.
2. **Read & count verification pipeline (`--mode=read` or `--mode=all`)**: Reads all written SSTables via `ReadFromSSTable(keys_only=True)`, counts total records via `Count.Globally()`, and verifies the count matches `--num_records`.

### Local Smoke Test (`DirectRunner`)

```bash
.venv/bin/python -m apache_beam_sstable.dataflow_load_test \
  --runner=DirectRunner \
  --num_records=5000 \
  --output_prefix=/tmp/sstable-smoke/data
```

### Cloud Dataflow ~100 GB Load Test (`DataflowRunner`)

```bash
# 1. Build the Linux x86_64 wheel if not already built
./scripts/build_linux_wheel.sh
WHEEL_PATH=$(ls wheelhouse/apache_beam_sstable-*manylinux*_x86_64.whl | head -n 1)

# 2. Submit the write and read verification pipelines to Cloud Dataflow
.venv/bin/python -m apache_beam_sstable.dataflow_load_test \
  --runner=DataflowRunner \
  --project=<GCP_PROJECT> \
  --region=<GCP_REGION> \
  --temp_location=gs://<BUCKET>/temp \
  --staging_location=gs://<BUCKET>/staging \
  --output_prefix=gs://<BUCKET>/sstable-loadtest/data \
  --num_records=100000000 \
  --key_size=24 \
  --value_size=1000 \
  --max_bytes_per_shard=536870912 \
  --extra_package="${WHEEL_PATH}"
```

---

## Vendored Upstream Code & Minimal Modifications

The `csrc/xla/tsl` and `csrc/tsl` directories are vendored reproducibly from TensorFlow [`v2.21.0`](https://github.com/tensorflow/tensorflow/tree/v2.21.0/third_party/xla/xla/tsl/lib/io) via [`scripts/vendor_tsl.py`](scripts/vendor_tsl.py), which downloads the upstream files and applies [`scripts/tsl_platform_standalone.patch`](scripts/tsl_platform_standalone.patch) (pass `--no-patch` to fetch only the unpatched upstream sources).

- **100% Unmodified Core Implementation**: All 18 files in `csrc/xla/tsl/lib/io/*`, 2 files in `csrc/xla/tsl/lib/hash/*`, and all 16 files in `csrc/tsl/platform/*` (including `tstring.h` and `snappy.h`) are **completely unmodified** from upstream `v2.21.0`.
- **Minimal Header-Only Dependency Pruning (4 files in [`scripts/tsl_platform_standalone.patch`](scripts/tsl_platform_standalone.patch))**: Only 4 headers under `csrc/xla/tsl/platform/` have surgical `#include` removals so the SSTable library compiles cleanly against standard `abseil-cpp` and `snappy` without pulling in `Eigen`, `ml_dtypes`, or `libprotobuf` (`error_codes.pb.h`):
  1. `csrc/xla/tsl/platform/types.h`: Removed `#include "tsl/platform/bfloat16.h"` and `#include "tsl/platform/ml_dtypes.h"` (floating-point tensor types unused by SSTable).
  2. `csrc/xla/tsl/platform/status.h`: Removed `#include "xla/tsl/protobuf/error_codes.pb.h"` and legacy `tensorflow::error::Code` protobuf enum comparison operators (TSL uses `absl::Status` and `absl::StatusCode` natively).
  3. `csrc/xla/tsl/platform/errors.h`: Removed `using tensorflow::error::*` protobuf enum aliases.
  4. `csrc/xla/tsl/platform/env.h`: Removed `#include "google/protobuf/*"` / `#include "tsl/platform/protobuf.h"` and `ReadBinaryProto` / `WriteBinaryProto` / `ReadTextProto` / `WriteTextProto` helper declarations.
