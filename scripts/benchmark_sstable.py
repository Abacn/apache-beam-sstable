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

"""Throughput and latency benchmarks for apache-beam-sstable."""

from __future__ import annotations

from pathlib import Path
import random
import tempfile
import time

from apache_beam_sstable.sstable import (
    CompressionType,
    SortingSSTableBuilder,
    SSTableBuilder,
    SSTableReader,
)


def run_benchmark(num_records: int = 200_000, value_size: int = 128) -> None:
  print(f"Preparing {num_records:,} records (value_size={value_size} bytes)...")
  sorted_records = [
      (f"bench_key_{i:010d}".encode(), (f"v{i:08d}_" * 16)[:value_size].encode())
      for i in range(num_records)
  ]
  total_raw_bytes = sum(len(k) + len(v) for k, v in sorted_records)
  total_raw_mb = total_raw_bytes / (1024 * 1024)

  rng = random.Random(42)
  unsorted_records = list(sorted_records)
  rng.shuffle(unsorted_records)

  with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = Path(tmp_dir)
    direct_sst = tmp_path / "direct.sst"
    sorted_sst = tmp_path / "sorted.sst"

    # 1. Pre-sorted SSTableBuilder write benchmark
    t0 = time.perf_counter()
    with SSTableBuilder(
        direct_sst, block_size=262144, compression=CompressionType.SNAPPY
    ) as builder:
      for k, v in sorted_records:
        builder.add(k, v)
    t_direct = time.perf_counter() - t0
    print(
        f"[SSTableBuilder] Wrote {num_records:,} records "
        f"({total_raw_mb:.1f} MiB raw -> {direct_sst.stat().st_size / (1024*1024):.1f} MiB sst) "
        f"in {t_direct:.3f}s ({num_records / t_direct:,.0f} rec/s, {total_raw_mb / t_direct:.1f} MiB/s)"
    )

    # 2. Unsorted SortingSSTableBuilder write benchmark
    t0 = time.perf_counter()
    with SortingSSTableBuilder(
        sorted_sst,
        block_size=262144,
        compression=CompressionType.SNAPPY,
        max_sorting_memory_bytes=256 * 1024 * 1024,
        temp_dir=str(tmp_path),
    ) as sorting_builder:
      sorting_builder.add_batch(unsorted_records)
    t_sort = time.perf_counter() - t0
    print(
        f"[SortingSSTableBuilder] Sorted & wrote {num_records:,} records "
        f"(spills={sorting_builder.num_spills}) in {t_sort:.3f}s "
        f"({num_records / t_sort:,.0f} rec/s, {total_raw_mb / t_sort:.1f} MiB/s)"
    )

    # 3. Full sequential scan benchmark (iterator)
    t0 = time.perf_counter()
    count = 0
    with SSTableReader(direct_sst, use_mmap=True) as reader:
      for _ in reader:
        count += 1
    t_scan = time.perf_counter() - t0
    assert count == num_records
    print(
        f"[SSTableReader iter] Scanned {count:,} records in {t_scan:.3f}s "
        f"({count / t_scan:,.0f} rec/s, {total_raw_mb / t_scan:.1f} MiB/s)"
    )

    # 4. Block-by-block bulk decode benchmark (used by _SSTableSource)
    t0 = time.perf_counter()
    block_count = 0
    with SSTableReader(direct_sst, use_mmap=True) as reader:
      for blk in reader.block_entries():
        block_count += len(
            reader.read_block_records(blk.block_offset, blk.block_size)
        )
    t_block_scan = time.perf_counter() - t0
    assert block_count == num_records
    print(
        f"[SSTableReader read_block_records] Scanned {block_count:,} records in {t_block_scan:.3f}s "
        f"({block_count / t_block_scan:,.0f} rec/s, {total_raw_mb / t_block_scan:.1f} MiB/s)"
    )

    # 5. Random point lookup benchmark
    sample_keys = [
        sorted_records[rng.randrange(num_records)][0] for _ in range(10_000)
    ]
    t0 = time.perf_counter()
    with SSTableReader(direct_sst, use_mmap=True) as reader:
      for k in sample_keys:
        val = reader.get(k)
        assert val is not None
    t_lookup = time.perf_counter() - t0
    print(
        f"[SSTableReader.get] 10,000 random point lookups in {t_lookup:.3f}s "
        f"({len(sample_keys) / t_lookup:,.0f} lookups/s, "
        f"{t_lookup * 1e6 / len(sample_keys):.1f} us/lookup)"
    )


if __name__ == "__main__":
  run_benchmark()
