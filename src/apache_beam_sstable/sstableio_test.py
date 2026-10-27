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

"""Unit and integration tests for Apache Beam SSTable IO transforms."""

from __future__ import annotations

from pathlib import Path
import random
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io import source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from apache_beam_sstable.sstable import (
    CompressionType,
    SSTableBuilder,
    SSTableReader,
)
from apache_beam_sstable.sstableio import (
    LookupInSSTable,
    ReadAllFromSSTable,
    ReadFromSSTable,
    WriteToSSTable,
    _SSTableSource,
)


def _write_test_sstable(
    path: Path,
    num_records: int = 200,
    block_size: int = 256,
    compression: CompressionType = CompressionType.SNAPPY,
) -> list[tuple[bytes, bytes]]:
  records = [
      (f"key_{i:06d}".encode(), f"val_{i:06d}_payload_data".encode())
      for i in range(num_records)
  ]
  with SSTableBuilder(
      path, block_size=block_size, compression=compression
  ) as builder:
    for k, v in records:
      builder.add(k, v)
  return records


class SSTableIOTest(unittest.TestCase):
  """Tests for `_SSTableSource`, `ReadFromSSTable`, `ReadAllFromSSTable`, `WriteToSSTable`, and `LookupInSSTable`."""

  def setUp(self) -> None:
    super().setUp()
    self._temp_dir = tempfile.TemporaryDirectory()
    self.tmp_path = Path(self._temp_dir.name)

  def tearDown(self) -> None:
    self._temp_dir.cleanup()
    super().tearDown()

  def test_sstable_source_initial_splitting_and_reentrant_reads(self) -> None:
    sst_path = self.tmp_path / "splittable.sst"
    expected = _write_test_sstable(sst_path, num_records=500, block_size=256)

    source = _SSTableSource(str(sst_path), min_bundle_size=0, splittable=True)
    self.assertEqual(source_test_utils.read_from_source(source), expected)

    splits = list(source.split(desired_bundle_size=1024))
    self.assertGreater(len(splits), 2)

    split_records = []
    for bundle in splits:
      split_records.extend(
          source_test_utils.read_from_source(
              bundle.source, bundle.start_position, bundle.stop_position
          )
      )
    self.assertEqual(split_records, expected)

    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_sstable_source_dynamic_splitting_exhaustive(self) -> None:
    sst_path = self.tmp_path / "dynamic_split.sst"
    expected = _write_test_sstable(
        sst_path,
        num_records=24,
        block_size=128,
        compression=CompressionType.NONE,
    )

    source = _SSTableSource(str(sst_path), min_bundle_size=0, splittable=True)
    self.assertEqual(source_test_utils.read_from_source(source), expected)
    source_test_utils.assert_split_at_fraction_exhaustive(source)

  def test_read_from_sstable_pipeline(self) -> None:
    sst_path = self.tmp_path / "input.sst"
    expected = _write_test_sstable(sst_path, num_records=1000, block_size=512)

    # 1. Full read with small min_bundle_size to exercise sub-file splits
    with TestPipeline() as p:
      actual = p | "ReadAll" >> ReadFromSSTable(
          str(sst_path), min_bundle_size=1024
      )
      assert_that(actual, equal_to(expected))

    # 2. Prefix filter + keys_only
    expected_prefix_keys = [
        k for k, _ in expected if k.startswith(b"key_0004")
    ]
    with TestPipeline() as p:
      prefix_keys = p | "ReadPrefixKeys" >> ReadFromSSTable(
          str(sst_path),
          min_bundle_size=1024,
          key_prefix=b"key_0004",
          keys_only=True,
      )
      assert_that(prefix_keys, equal_to(expected_prefix_keys))

    # 3. Key range filter [start_key, end_key)
    expected_range = [
        (k, v)
        for k, v in expected
        if b"key_000250" <= k < b"key_000600"
    ]
    with TestPipeline() as p:
      range_kv = p | "ReadRange" >> ReadFromSSTable(
          str(sst_path),
          min_bundle_size=1024,
          start_key=b"key_000250",
          end_key=b"key_000600",
      )
      assert_that(range_kv, equal_to(expected_range))

  def test_read_all_from_sstable_pipeline(self) -> None:
    path_a = self.tmp_path / "part_a.sst"
    path_b = self.tmp_path / "part_b.sst"
    expected_a = _write_test_sstable(path_a, num_records=150, block_size=256)
    expected_b = _write_test_sstable(path_b, num_records=150, block_size=256)

    with TestPipeline() as p:
      files = p | beam.Create([str(path_a), str(path_b)])
      records = files | ReadAllFromSSTable(
          min_bundle_size=512, desired_bundle_size=1024
      )
      assert_that(records, equal_to(expected_a + expected_b))

  def test_write_to_sstable_dynamic_shards_and_lookup_pipeline(self) -> None:
    rng = random.Random(123)
    records = [
        (f"user_{i:06d}".encode(), f"record_location_{i:06d}".encode())
        for i in range(1500)
    ]
    shuffled = list(records)
    rng.shuffle(shuffled)

    output_prefix = str(self.tmp_path / "dynamic_index")

    with TestPipeline() as p:
      _ = (
          p
          | "CreateUnsorted" >> beam.Create(shuffled)
          | "WriteSSTable"
          >> WriteToSSTable(
              file_path_prefix=output_prefix,
              file_name_suffix=".sst",
              block_size=1024,
              max_sorting_memory_mb=1,
              temp_dir=str(self.tmp_path),
              max_records_per_shard=400,
          )
      )

    shard_files = sorted(self.tmp_path.glob("dynamic_index-*-of-*.sst"))
    self.assertGreaterEqual(len(shard_files), 4)

    all_read_back = []
    for shard_file in shard_files:
      with SSTableReader(shard_file) as reader:
        shard_items = list(reader)
        self.assertLessEqual(len(shard_items), 400)
        # Every individual SSTable shard is strictly sorted by key
        self.assertEqual(shard_items, sorted(shard_items, key=lambda kv: kv[0]))
        all_read_back.extend(shard_items)

    self.assertEqual(sorted(all_read_back, key=lambda kv: kv[0]), records)

    with TestPipeline() as p:
      read_sharded = p | "ReadShardedGlob" >> ReadFromSSTable(
          f"{output_prefix}-*-of-*.sst", min_bundle_size=1024
      )
      assert_that(read_sharded, equal_to(records))

    lookup_keys = [
        b"user_000000",
        b"user_000777",
        b"user_001499",
        b"missing_key",
    ]
    expected_lookups = [
        (b"user_000000", b"record_location_000000"),
        (b"user_000777", b"record_location_000777"),
        (b"user_001499", b"record_location_001499"),
        (b"missing_key", None),
    ]
    with TestPipeline() as p:
      lookup_results = (
          p
          | "CreateLookupKeys" >> beam.Create(lookup_keys)
          | "PointLookup"
          >> LookupInSSTable(file_pattern=f"{output_prefix}-*-of-*.sst")
      )
      assert_that(lookup_results, equal_to(expected_lookups))

  def test_write_to_sstable_max_bytes_rollover_and_empty_handling(self) -> None:
    records = [
        (f"k_{i:05d}".encode(), (f"v_{i:05d}_" * 20).encode())
        for i in range(200)
    ]
    bytes_prefix = str(self.tmp_path / "bytes_rollover")
    with TestPipeline() as p:
      _ = (
          p
          | "CreateRecords" >> beam.Create(records)
          | "WriteByBytes"
          >> WriteToSSTable(
              file_path_prefix=bytes_prefix,
              file_name_suffix=".sst",
              max_bytes_per_shard=8 * 1024,
          )
      )

    byte_shards = sorted(self.tmp_path.glob("bytes_rollover-*-of-*.sst"))
    self.assertGreaterEqual(len(byte_shards), 3)

    # Empty PCollection with skip_if_empty=True writes 0 shards
    empty_skip_prefix = str(self.tmp_path / "empty_skip")
    with TestPipeline() as p:
      _ = (
          p
          | "CreateEmptySkip" >> beam.Create([])
          | "WriteEmptySkip"
          >> WriteToSSTable(
              file_path_prefix=empty_skip_prefix,
              file_name_suffix=".sst",
              skip_if_empty=True,
          )
      )
    self.assertEqual(list(self.tmp_path.glob("empty_skip*")), [])


if __name__ == "__main__":
  unittest.main()

