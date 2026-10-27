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

"""Unit tests for the C++ SSTable bindings and low-level Python API."""

from __future__ import annotations

import io
from pathlib import Path
import random
import tempfile
import unittest

from apache_beam_sstable.sstable import (
    TABLE_MAGIC_NUMBER,
    CompressionType,
    SortingSSTableBuilder,
    SSTableBuilder,
    SSTableReader,
    crc32c_mask,
    crc32c_value,
    inspect_table_blocks,
)


class SSTableTest(unittest.TestCase):
  """Tests for C++ SSTable bindings (`_sstable_cc`) and `apache_beam_sstable.sstable`."""

  def setUp(self) -> None:
    super().setUp()
    self._temp_dir = tempfile.TemporaryDirectory()
    self.tmp_path = Path(self._temp_dir.name)

  def tearDown(self) -> None:
    self._temp_dir.cleanup()
    super().tearDown()

  def test_table_magic_and_crc32c(self) -> None:
    self.assertEqual(TABLE_MAGIC_NUMBER, 0xDB4775248B80FB57)
    crc = crc32c_value(b"hello-sstable")
    masked = crc32c_mask(crc)
    self.assertIsInstance(crc, int)
    self.assertIsInstance(masked, int)
    self.assertNotEqual(crc, masked)

  def test_builder_and_reader_roundtrip(self) -> None:
    for compression in (CompressionType.NONE, CompressionType.SNAPPY):
      for use_mmap in (False, True):
        with self.subTest(compression=compression, use_mmap=use_mmap):
          sst_path = (
              self.tmp_path / f"roundtrip_{int(compression)}_{int(use_mmap)}.sst"
          )
          records = [
              (f"key_{i:06d}".encode(), f"value_payload_{i:06d}_" * 8)
              for i in range(2000)
          ]
          expected = [(k, v.encode()) for k, v in records]

          with SSTableBuilder(
              sst_path, block_size=1024, compression=compression
          ) as builder:
            for k, v in expected:
              builder.add(k, v)
            self.assertEqual(builder.num_entries, 2000)

          self.assertTrue(sst_path.exists())
          self.assertGreater(sst_path.stat().st_size, 48)

          blocks = inspect_table_blocks(sst_path, use_mmap=use_mmap)
          self.assertGreater(len(blocks), 1)
          self.assertEqual(blocks[0].block_offset, 0)

          with SSTableReader(sst_path, use_mmap=use_mmap) as reader:
            self.assertEqual(reader.file_size, sst_path.stat().st_size)

            # Point lookups
            self.assertEqual(reader.get(b"key_000000"), expected[0][1])
            self.assertEqual(reader[b"key_001234"], expected[1234][1])
            self.assertIn(b"key_001999", reader)
            self.assertNotIn(b"key_999999", reader)
            self.assertIsNone(reader.get(b"key_999999"))
            self.assertEqual(
                reader.get(b"key_999999", b"fallback"), b"fallback"
            )
            with self.assertRaises(KeyError):
              _ = reader[b"key_999999"]

            # Full scan
            self.assertEqual(list(reader), expected)
            self.assertEqual(list(reader.keys()), [k for k, _ in expected])
            self.assertEqual(list(reader.values()), [v for _, v in expected])

            # Prefix scan
            prefix_expected = [
                (k, v) for k, v in expected if k.startswith(b"key_00012")
            ]
            self.assertEqual(
                list(reader.iter_prefix(b"key_00012")), prefix_expected
            )
            self.assertEqual(
                list(reader.iter_prefix(b"key_00012", keys_only=True)),
                [k for k, _ in prefix_expected],
            )

            # Range scan [start_key, end_key)
            range_expected = [
                (k, v)
                for k, v in expected
                if b"key_000500" <= k < b"key_000750"
            ]
            self.assertEqual(
                list(reader.iter_range(b"key_000500", b"key_000750")),
                range_expected,
            )

            # Block-by-block reading matches full table
            reconstructed = []
            for blk in reader.block_entries():
              reconstructed.extend(
                  reader.read_block_records(blk.block_offset, blk.block_size)
              )
            self.assertEqual(reconstructed, expected)

  def test_empty_sstable(self) -> None:
    sst_path = self.tmp_path / "empty.sst"
    with SSTableBuilder(sst_path) as builder:
      self.assertEqual(builder.num_entries, 0)

    blocks = inspect_table_blocks(sst_path)
    self.assertEqual(blocks, [])

    with SSTableReader(sst_path) as reader:
      self.assertEqual(list(reader), [])
      self.assertIsNone(reader.get(b"any"))
      self.assertNotIn(b"any", reader)

  def test_python_file_like_object_roundtrip(self) -> None:
    buf = io.BytesIO()
    items = [(f"k_{i:04d}".encode(), f"v_{i:04d}".encode()) for i in range(500)]

    with SSTableBuilder(buf, block_size=512) as builder:
      for k, v in items:
        builder.add(k, v)

    raw_bytes = buf.getvalue()
    self.assertGreater(len(raw_bytes), 48)

    read_buf = io.BytesIO(raw_bytes)
    blocks = inspect_table_blocks(read_buf)
    self.assertGreater(len(blocks), 1)

    with SSTableReader(read_buf) as reader:
      self.assertEqual(list(reader), items)
      self.assertEqual(reader.get(b"k_0250"), b"v_0250")
      self.assertIsNone(reader.get(b"k_9999"))

  def test_out_of_order_key_rejected_in_sstable_builder(self) -> None:
    sst_path = self.tmp_path / "out_of_order.sst"
    builder = SSTableBuilder(sst_path)
    builder.add(b"key_b", b"val_b")
    with self.assertRaisesRegex(ValueError, "strictly increasing keys"):
      builder.add(b"key_a", b"val_a")
    builder.abandon()

  def test_corrupted_sstable_detected(self) -> None:
    sst_path = self.tmp_path / "corrupt.sst"
    with SSTableBuilder(
        sst_path, block_size=256, compression=CompressionType.NONE
    ) as builder:
      for i in range(100):
        builder.add(f"key_{i:04d}".encode(), f"val_{i:04d}".encode())

    data = bytearray(sst_path.read_bytes())
    data[10] ^= 0xFF
    sst_path.write_bytes(bytes(data))

    with SSTableReader(sst_path) as reader:
      with self.assertRaisesRegex(IOError, "block checksum mismatch"):
        _ = list(reader)

  def test_sorting_sstable_builder_in_memory_and_spilling(self) -> None:
    rng = random.Random(42)
    raw_items = [
        (f"key_{i:06d}".encode(), f"payload_{i:06d}_" * 6)
        for i in range(5000)
    ]
    expected = [(k, v.encode()) for k, v in raw_items]
    shuffled = list(expected)
    rng.shuffle(shuffled)

    # 1. In-memory sort (large memory budget -> 0 spill files)
    in_mem_path = self.tmp_path / "sorted_in_mem.sst"
    with SortingSSTableBuilder(
        in_mem_path,
        block_size=2048,
        max_sorting_memory_bytes=64 * 1024 * 1024,
        temp_dir=str(self.tmp_path),
    ) as builder:
      builder.add_batch(shuffled)
      self.assertEqual(builder.total_added_entries, 5000)

    self.assertEqual(builder.num_spills, 0)
    self.assertEqual(builder.num_entries, 5000)
    with SSTableReader(in_mem_path) as reader:
      self.assertEqual(list(reader), expected)

    # 2. Multi-spill external K-way merge (tiny memory budget -> multiple spills)
    spilled_path = self.tmp_path / "sorted_spilled.sst"
    with SortingSSTableBuilder(
        spilled_path,
        block_size=2048,
        max_sorting_memory_bytes=64 * 1024,  # 64 KiB budget forces multiple spills
        temp_dir=str(self.tmp_path),
    ) as builder:
      for k, v in shuffled:
        builder.add(k, v)

    self.assertGreaterEqual(builder.num_spills, 3)
    self.assertEqual(builder.num_entries, 5000)
    with SSTableReader(spilled_path) as reader:
      self.assertEqual(list(reader), expected)

  def test_sorting_sstable_builder_deduplication(self) -> None:
    dedup_path = self.tmp_path / "dedup.sst"
    with SortingSSTableBuilder(
        dedup_path,
        max_sorting_memory_bytes=64 * 1024,
        temp_dir=str(self.tmp_path),
        deduplicate_keys=True,
    ) as builder:
      for round_idx in range(3):
        for i in range(500):
          builder.add(
              f"dup_key_{i:04d}".encode(),
              f"round_{round_idx}_val_{i:04d}".encode(),
          )

    self.assertEqual(builder.total_added_entries, 1500)
    self.assertEqual(builder.num_entries, 500)
    with SSTableReader(dedup_path) as reader:
      actual = list(reader)
      self.assertEqual(len(actual), 500)
      for i, (k, v) in enumerate(actual):
        self.assertEqual(k, f"dup_key_{i:04d}".encode())
        self.assertEqual(v, f"round_2_val_{i:04d}".encode())


if __name__ == "__main__":
  unittest.main()
