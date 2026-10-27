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

"""
Cloud Dataflow load test for Apache Beam SSTable IO (~100 GB write + read & count verification).
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any, Iterator

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.util import assert_that, equal_to

from apache_beam_sstable.sstable import CompressionType
from apache_beam_sstable.sstableio import ReadFromSSTable, WriteToSSTable

_LOGGER = logging.getLogger(__name__)


class _VerifyRecordCountDoFn(beam.DoFn):
  """Logs and verifies that the actual record count matches ``expected_count``."""

  def __init__(self, expected_count: int) -> None:
    self._expected_count = expected_count

  def process(self, actual_count: int) -> Iterator[int]:
    _LOGGER.info(
        "SSTable load test read count verification: actual=%d, expected=%d",
        actual_count,
        self._expected_count,
    )
    if actual_count != self._expected_count:
      raise RuntimeError(
          f"SSTable record count mismatch: expected {self._expected_count}, "
          f"got {actual_count}"
      )
    yield actual_count


def _build_synthetic_source_spec(
    num_records: int,
    key_size: int,
    value_size: int,
    num_initial_bundles: int,
) -> dict[str, Any]:
  spec: dict[str, Any] = {
      "numRecords": num_records,
      "keySizeBytes": key_size,
      "valueSizeBytes": value_size,
      "algorithm": "lcg",
  }
  if num_initial_bundles > 0:
    spec["forceNumInitialBundles"] = num_initial_bundles
  return spec


def run_load_test(argv: list[str] | None = None) -> None:
  parser = argparse.ArgumentParser(
      description="Run Apache Beam SSTable IO load test (~100 GB write + read verification)."
  )
  parser.add_argument(
      "--output_prefix",
      default="gs://clouddfe-yihu-test/sstable-loadtest/data",
      help="Output SSTable file path prefix (default: %(default)s).",
  )
  parser.add_argument(
      "--num_records",
      type=int,
      default=100_000_000,
      help="Total number of synthetic records to write and read (default: %(default)s, ~100 GB at 1024 B/record).",
  )
  parser.add_argument(
      "--key_size",
      type=int,
      default=24,
      help="Key size in bytes (default: %(default)s).",
  )
  parser.add_argument(
      "--value_size",
      type=int,
      default=1000,
      help="Value size in bytes (default: %(default)s).",
  )
  parser.add_argument(
      "--num_source_splits",
      type=int,
      default=0,
      help="Optional forceNumInitialBundles for SyntheticSource; 0 lets the runner split dynamically (default: %(default)s).",
  )
  parser.add_argument(
      "--max_bytes_per_shard",
      type=int,
      default=512 * 1024 * 1024,
      help="Maximum uncompressed key-value bytes per SSTable shard before rollover (default: 512 MiB).",
  )
  parser.add_argument(
      "--max_sorting_memory_mb",
      type=int,
      default=256,
      help="C++ SortingSSTableBuilder in-memory arena budget in MiB (default: %(default)s).",
  )
  parser.add_argument(
      "--block_size",
      type=int,
      default=262144,
      help="SSTable data block size in bytes (default: %(default)s).",
  )
  parser.add_argument(
      "--compression",
      choices=["SNAPPY", "NONE"],
      default="SNAPPY",
      help="SSTable block compression codec (default: %(default)s).",
  )
  parser.add_argument(
      "--mode",
      choices=["all", "write", "read"],
      default="all",
      help=(
          "'all' runs the WriteToSSTable pipeline followed by the ReadFromSSTable pipeline; "
          "'write' runs only the write pipeline; 'read' runs only the read pipeline (default: %(default)s)."
      ),
  )

  known_args, pipeline_args = parser.parse_known_args(argv)
  compression = (
      CompressionType.SNAPPY
      if known_args.compression == "SNAPPY"
      else CompressionType.NONE
  )
  total_raw_gb = (
      known_args.num_records
      * (known_args.key_size + known_args.value_size)
      / (1024**3)
  )
  _LOGGER.info(
      "Starting SSTable load test: mode=%s, num_records=%d (~%.2f GiB raw), "
      "output_prefix=%s.sst, compression=%s",
      known_args.mode,
      known_args.num_records,
      total_raw_gb,
      known_args.output_prefix,
      known_args.compression,
  )

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  synthetic_spec = _build_synthetic_source_spec(
      num_records=known_args.num_records,
      key_size=known_args.key_size,
      value_size=known_args.value_size,
      num_initial_bundles=known_args.num_source_splits,
  )

  start_time = time.perf_counter()

  if known_args.mode in ("all", "write"):
    with beam.Pipeline(options=pipeline_options) as p:
      _ = (
          p
          | "ReadSyntheticSource"
          >> beam.io.Read(SyntheticSource(synthetic_spec))
          | "WriteSSTables"
          >> WriteToSSTable(
              file_path_prefix=known_args.output_prefix,
              file_name_suffix=".sst",
              block_size=known_args.block_size,
              compression=compression,
              max_sorting_memory_mb=known_args.max_sorting_memory_mb,
              max_bytes_per_shard=known_args.max_bytes_per_shard,
          )
      )

  if known_args.mode in ("all", "read"):
    file_pattern = f"{known_args.output_prefix}-*-of-*.sst"
    with beam.Pipeline(options=pipeline_options) as p:
      read_count = (
          p
          | "ReadSSTables"
          >> ReadFromSSTable(file_pattern=file_pattern, keys_only=True)
          | "CountRecords" >> beam.combiners.Count.Globally()
          | "VerifyCount"
          >> beam.ParDo(_VerifyRecordCountDoFn(known_args.num_records))
      )
      assert_that(read_count, equal_to([known_args.num_records]))

  elapsed = time.perf_counter() - start_time
  _LOGGER.info("SSTable load test completed successfully in %.2f seconds.", elapsed)


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  run_load_test()
