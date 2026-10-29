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

"""Apache Beam Python SDK IO connector for SSTable files."""

from __future__ import annotations

from collections import OrderedDict
import functools
import logging
import os
from typing import Any, Iterator, Optional, Union

import apache_beam as beam
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.pvalue import EmptySideInput
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem

from apache_beam_sstable.sstable import (
    CompressionType,
    SortingSSTableBuilder,
    SSTableReader,
)

def _is_local_path(path: str) -> bool:
  """Returns True if ``path`` refers to the local OS filesystem."""
  return "://" not in path


class _SSTableSource(filebasedsource.FileBasedSource):
  """Splittable ``FileBasedSource`` for reading SSTable files at block boundaries.

  Every SSTable data block start offset (``block.block_offset``) is treated as a
  split point for Beam's ``OffsetRangeTracker``. At read time, only the 48-byte
  SSTable Footer and Index Block are read first to discover exact block offsets
  and separator keys, enabling:
  1. Sub-file byte range splitting and dynamic work rebalancing with zero
     duplicate or skipped records.
  2. Block-level key range pruning using ``separator_key`` before touching data
     blocks on disk or remote object storage.
  """

  def __init__(
      self,
      file_pattern: str,
      min_bundle_size: int = 64 * 1024 * 1024,
      splittable: bool = True,
      validate: bool = True,
      key_prefix: Optional[bytes] = None,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      use_mmap: bool = False,
  ) -> None:
    super().__init__(
        file_pattern=file_pattern,
        min_bundle_size=min_bundle_size,
        compression_type=CompressionTypes.UNCOMPRESSED,
        splittable=splittable,
        validate=validate,
    )
    if key_prefix is not None and not isinstance(key_prefix, bytes):
      raise TypeError(f"key_prefix must be bytes, got {type(key_prefix).__name__}")
    if start_key is not None and not isinstance(start_key, bytes):
      raise TypeError(f"start_key must be bytes, got {type(start_key).__name__}")
    if end_key is not None and not isinstance(end_key, bytes):
      raise TypeError(f"end_key must be bytes, got {type(end_key).__name__}")

    self._key_prefix = key_prefix
    self._start_key = start_key
    self._end_key = end_key
    self._use_mmap = bool(use_mmap)

  def read_records(
      self, file_name: str, range_tracker: OffsetRangeTracker
  ) -> Iterator[Any]:
    start_offset = range_tracker.start_position()
    stop_offset = range_tracker.stop_position()
    if start_offset >= stop_offset:
      return

    # Compute effective lower key bound for index-level block pruning.
    effective_start_key = self._start_key
    if self._key_prefix is not None:
      if effective_start_key is None or self._key_prefix > effective_start_key:
        effective_start_key = self._key_prefix

    if _is_local_path(file_name) and os.path.exists(file_name):
      with SSTableReader(
          file_name, use_mmap=self._use_mmap, block_cache_size=0
      ) as reader:
        yield from self._read_blocks_from_reader(
            reader, range_tracker, start_offset, effective_start_key
        )
    else:
      with self.open_file(file_name) as f:
        with SSTableReader(f, block_cache_size=0) as reader:
          yield from self._read_blocks_from_reader(
              reader, range_tracker, start_offset, effective_start_key
          )

  def _read_blocks_from_reader(
      self,
      reader: SSTableReader,
      range_tracker: OffsetRangeTracker,
      start_offset: int,
      effective_start_key: Optional[bytes],
  ) -> Iterator[Any]:
    blocks = reader.block_entries()
    if not blocks:
      return

    for block in blocks:
      block_offset = block.block_offset
      if block_offset < start_offset:
        continue

      if not range_tracker.try_claim(block_offset):
        break

      # Index-level pruning: every key k in `block` satisfies k <= block.separator_key.
      if (
          effective_start_key is not None
          and block.separator_key < effective_start_key
      ):
        continue

      records = reader.read_block_records(
          block_offset=block_offset,
          block_size=block.block_size,
          start_key=self._start_key,
          end_key=self._end_key,
          key_prefix=self._key_prefix,
      )
      for record in records:
        yield record


class ReadFromSSTable(PTransform):
  """A ``PTransform`` for reading SSTable files into a ``PCollection[(bytes, bytes)]``."""

  def __init__(
      self,
      file_pattern: str,
      min_bundle_size: int = 64 * 1024 * 1024,
      splittable: bool = True,
      validate: bool = True,
      key_prefix: Optional[bytes] = None,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      use_mmap: bool = False,
  ) -> None:
    super().__init__()
    self._source = _SSTableSource(
        file_pattern=file_pattern,
        min_bundle_size=min_bundle_size,
        splittable=splittable,
        validate=validate,
        key_prefix=key_prefix,
        start_key=start_key,
        end_key=end_key,
        use_mmap=use_mmap,
    )

  def expand(self, pbegin: beam.pvalue.PBegin) -> beam.PCollection:
    return pbegin | beam.io.Read(self._source)


class ReadAllFromSSTable(PTransform):
  """A ``PTransform`` that reads SSTable files from a ``PCollection[str]`` of file patterns."""

  def __init__(
      self,
      min_bundle_size: int = 64 * 1024 * 1024,
      desired_bundle_size: int = 64 * 1024 * 1024,
      splittable: bool = True,
      key_prefix: Optional[bytes] = None,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      use_mmap: bool = False,
      with_filename: bool = False,
  ) -> None:
    super().__init__()
    source_from_file = functools.partial(
        _SSTableSource,
        min_bundle_size=min_bundle_size,
        splittable=splittable,
        validate=False,
        key_prefix=key_prefix,
        start_key=start_key,
        end_key=end_key,
        use_mmap=use_mmap,
    )
    self._read_all = filebasedsource.ReadAllFiles(
        splittable=splittable,
        compression_type=CompressionTypes.UNCOMPRESSED,
        desired_bundle_size=desired_bundle_size,
        min_bundle_size=min_bundle_size,
        source_from_file=source_from_file,
        with_filename=with_filename,
    )

  def expand(self, pcoll: beam.PCollection[str]) -> beam.PCollection:
    return pcoll | self._read_all


class _SSTableShardHandle:
  """Opaque shard writer handle returned by ``_SSTableSink.open()``."""

  def __init__(
      self,
      builder: SortingSSTableBuilder,
      raw_stream: Optional[Any] = None,
  ) -> None:
    self.builder = builder
    self.raw_stream = raw_stream
    self.bytes_written: int = 0
    self._closed: bool = False

  def add(self, key: bytes, value: bytes) -> None:
    self.builder.add(key, value)
    self.bytes_written += len(key) + len(value)

  def close(self) -> None:
    if self._closed:
      return
    self._closed = True
    try:
      self.builder.finish()
    except Exception:
      self.builder.abandon()
      raise
    finally:
      if self.raw_stream is not None:
        self.raw_stream.close()


class _SSTableSinkWriter(filebasedsink.FileBasedSinkWriter):
  """Writer for ``_SSTableSink`` supporting record and byte capacity rollover."""

  def at_capacity(self) -> bool:
    return bool(
        (
            self.sink.max_records_per_shard
            and self.num_records_written >= self.sink.max_records_per_shard
        )
        or (
            self.sink.max_bytes_per_shard
            and self.temp_handle.bytes_written >= self.sink.max_bytes_per_shard
        )
    )


class _SSTableSink(filebasedsink.FileBasedSink):
  """A ``FileBasedSink`` for writing sorted SSTable shards without fixed sharding.

  Uses runner-determined bundle sharding (``num_shards=0``) with optional
  per-shard rollover via ``max_records_per_shard`` and ``max_bytes_per_shard``.
  Each shard is sorted out-of-core in C++ via ``SortingSSTableBuilder``, written
  to the ``FileBasedSink`` temporary directory, and atomically finalized in
  parallel batches by ``FileBasedSink.finalize_write``.
  """

  def __init__(
      self,
      file_path_prefix: str,
      file_name_suffix: str = "",
      shard_name_template: Optional[
          str
      ] = filebasedsink.DEFAULT_SHARD_NAME_TEMPLATE,
      block_size: int = 262144,
      block_restart_interval: int = 16,
      compression: Union[CompressionType, int] = CompressionType.SNAPPY,
      max_sorting_memory_mb: int = 256,
      temp_dir: Optional[str] = None,
      deduplicate_keys: bool = False,
      write_buffer_size: int = 4 * 1024 * 1024,
      max_records_per_shard: Optional[int] = None,
      max_bytes_per_shard: Optional[int] = None,
      skip_if_empty: bool = False,
      triggering_frequency: Optional[int] = None,
  ) -> None:
    if shard_name_template == "":
      raise ValueError(
          "Fixed single-shard template '' is not supported; use a template "
          "containing 'S+' such as '-SSSSS-of-NNNNN'."
      )
    super().__init__(
        file_path_prefix=file_path_prefix,
        coder=None,
        file_name_suffix=file_name_suffix,
        num_shards=0,
        shard_name_template=shard_name_template,
        mime_type="application/octet-stream",
        compression_type=CompressionTypes.UNCOMPRESSED,
        max_records_per_shard=max_records_per_shard,
        max_bytes_per_shard=max_bytes_per_shard,
        skip_if_empty=skip_if_empty,
        triggering_frequency=triggering_frequency,
    )
    self._block_size = block_size
    self._block_restart_interval = block_restart_interval
    self._compression = int(compression)
    self._max_sorting_memory_mb = int(max_sorting_memory_mb)
    self._max_sorting_memory_bytes = self._max_sorting_memory_mb * 1024 * 1024
    self._temp_dir = temp_dir
    self._deduplicate_keys = bool(deduplicate_keys)
    self._write_buffer_size = write_buffer_size

  def display_data(self) -> dict[str, Any]:
    res = super().display_data()
    res["block_size"] = DisplayDataItem(self._block_size, label="Block Size")
    res["block_restart_interval"] = DisplayDataItem(
        self._block_restart_interval, label="Block Restart Interval"
    )
    res["sstable_compression"] = DisplayDataItem(
        CompressionType(self._compression).name, label="SSTable Compression"
    )
    res["max_sorting_memory_mb"] = DisplayDataItem(
        self._max_sorting_memory_mb, label="Max Sorting Memory (MB)"
    )
    res["deduplicate_keys"] = DisplayDataItem(
        self._deduplicate_keys, label="Deduplicate Keys"
    )
    return res

  def open_writer(self, init_result: Any, uid: str) -> _SSTableSinkWriter:
    if isinstance(init_result, EmptySideInput):
      file_path_prefix = self.file_path_prefix.get()
      init_result = self._create_temp_dir(file_path_prefix)
      FileSystems.mkdirs(init_result)

    file_path_prefix = self.file_path_prefix.get()
    file_name_suffix = self.file_name_suffix.get()
    suffix = "." + os.path.basename(file_path_prefix) + file_name_suffix
    writer_path = FileSystems.join(init_result, uid) + suffix
    return _SSTableSinkWriter(self, writer_path)

  def open(self, temp_path: str) -> _SSTableShardHandle:
    if _is_local_path(temp_path):
      parent_dir = os.path.dirname(os.path.abspath(temp_path))
      if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
      builder = SortingSSTableBuilder(
          temp_path,
          block_size=self._block_size,
          block_restart_interval=self._block_restart_interval,
          compression=self._compression,
          max_sorting_memory_bytes=self._max_sorting_memory_bytes,
          temp_dir=self._temp_dir,
          deduplicate_keys=self._deduplicate_keys,
          write_buffer_size=self._write_buffer_size,
      )
      return _SSTableShardHandle(builder=builder, raw_stream=None)

    raw_stream = FileSystems.create(
        temp_path,
        mime_type=self.mime_type,
        compression_type=CompressionTypes.UNCOMPRESSED,
    )
    try:
      builder = SortingSSTableBuilder(
          raw_stream,
          block_size=self._block_size,
          block_restart_interval=self._block_restart_interval,
          compression=self._compression,
          max_sorting_memory_bytes=self._max_sorting_memory_bytes,
          temp_dir=self._temp_dir,
          deduplicate_keys=self._deduplicate_keys,
          write_buffer_size=self._write_buffer_size,
      )
    except Exception:
      raw_stream.close()
      raise
    return _SSTableShardHandle(builder=builder, raw_stream=raw_stream)

  def write_record(
      self, file_handle: _SSTableShardHandle, value: tuple[bytes, bytes]
  ) -> None:
    key, val = value
    file_handle.add(key, val)

  def close(self, file_handle: Optional[_SSTableShardHandle]) -> None:
    if file_handle is not None:
      file_handle.close()


class WriteToSSTable(PTransform):
  """A ``PTransform`` for writing ``(key, value)`` pairs to sharded, sorted SSTable files.

  Features:
  - Runner-determined dynamic bundle sharding via ``_SSTableSink(FileBasedSink)``
    with optional per-shard rollover when ``max_records_per_shard`` or
    ``max_bytes_per_shard`` is reached (no fixed shard count or global
    ``GroupByKey`` bottleneck).
  - Bounded-memory external sorting per shard via C++ ``SortingSSTableBuilder``
    (buffers and sorts up to ``max_sorting_memory_mb`` in native C++ memory,
    spilling sorted runs to ``temp_dir`` and performing a K-way merge if a shard
    exceeds the memory budget).
  - Two-phase temporary directory staging, ``pre_finalize`` cleanup, and
    multi-threaded batched rename via ``FileBasedSink``.
  """

  def __init__(
      self,
      file_path_prefix: str,
      file_name_suffix: str = "",
      shard_name_template: Optional[
          str
      ] = filebasedsink.DEFAULT_SHARD_NAME_TEMPLATE,
      block_size: int = 262144,
      block_restart_interval: int = 16,
      compression: Union[CompressionType, int] = CompressionType.SNAPPY,
      max_sorting_memory_mb: int = 256,
      temp_dir: Optional[str] = None,
      deduplicate_keys: bool = False,
      write_buffer_size: int = 4 * 1024 * 1024,
      max_records_per_shard: Optional[int] = None,
      max_bytes_per_shard: Optional[int] = None,
      skip_if_empty: bool = False,
      triggering_frequency: Optional[int] = None,
  ) -> None:
    super().__init__()
    self._sink = _SSTableSink(
        file_path_prefix=file_path_prefix,
        file_name_suffix=file_name_suffix,
        shard_name_template=shard_name_template,
        block_size=block_size,
        block_restart_interval=block_restart_interval,
        compression=compression,
        max_sorting_memory_mb=max_sorting_memory_mb,
        temp_dir=temp_dir,
        deduplicate_keys=deduplicate_keys,
        write_buffer_size=write_buffer_size,
        max_records_per_shard=max_records_per_shard,
        max_bytes_per_shard=max_bytes_per_shard,
        skip_if_empty=skip_if_empty,
        triggering_frequency=triggering_frequency,
    )

  def expand(
      self, pcoll: beam.PCollection[tuple[bytes, bytes]]
  ) -> beam.PCollection[str]:
    if (
        not pcoll.is_bounded
        and self._sink.shard_name_template
        == filebasedsink.DEFAULT_SHARD_NAME_TEMPLATE
    ):
      self._sink.shard_name_template = (
          filebasedsink.DEFAULT_WINDOW_SHARD_NAME_TEMPLATE
      )
      self._sink.shard_name_format = self._sink._template_to_format(
          self._sink.shard_name_template
      )
      self._sink.shard_name_glob_format = self._sink._template_to_glob_format(
          self._sink.shard_name_template
      )
    return pcoll | beam.io.Write(self._sink)

  def display_data(self) -> dict[str, Any]:
    return {"sink_dd": self._sink}


class _LookupInSSTableDoFn(beam.DoFn):
  """Performs point lookups across SSTable files matching ``file_pattern``."""

  def __init__(
      self,
      file_pattern: str,
      default_value: Any = None,
      max_open_readers: int = 64,
      block_cache_size: int = 8 * 1024 * 1024,
      use_mmap: bool = False,
  ) -> None:
    self._file_pattern = file_pattern
    self._default_value = default_value
    self._max_open_readers = max(1, max_open_readers)
    self._block_cache_size = block_cache_size
    self._use_mmap = use_mmap
    self._shard_paths: list[str] = []
    self._shard_max_keys: dict[str, Optional[bytes]] = {}
    self._readers: OrderedDict[str, tuple[SSTableReader, Any]] = OrderedDict()

  def setup(self) -> None:
    self._readers = OrderedDict()
    match_results = FileSystems.match([self._file_pattern])
    self._shard_paths = sorted(
        metadata.path
        for mr in match_results
        for metadata in mr.metadata_list
    )
    self._shard_max_keys = {}

  def _get_reader(self, shard_path: str) -> SSTableReader:
    if shard_path in self._readers:
      self._readers.move_to_end(shard_path)
      return self._readers[shard_path][0]

    while len(self._readers) >= self._max_open_readers:
      _, (old_reader, old_file) = self._readers.popitem(last=False)
      old_reader.close()
      if old_file is not None:
        old_file.close()

    if _is_local_path(shard_path) and os.path.exists(shard_path):
      reader = SSTableReader(
          shard_path,
          use_mmap=self._use_mmap,
          block_cache_size=self._block_cache_size,
      )
      self._readers[shard_path] = (reader, None)
    else:
      file_obj = FileSystems.open(
          shard_path, compression_type=CompressionTypes.UNCOMPRESSED
      )
      reader = SSTableReader(
          file_obj,
          block_cache_size=self._block_cache_size,
      )
      self._readers[shard_path] = (reader, file_obj)

    if shard_path not in self._shard_max_keys:
      blocks = reader.block_entries()
      self._shard_max_keys[shard_path] = (
          blocks[-1].separator_key if blocks else None
      )
    return reader

  def process(self, key: bytes) -> Iterator[tuple[bytes, Any]]:
    for shard_path in self._shard_paths:
      if shard_path in self._shard_max_keys:
        max_key = self._shard_max_keys[shard_path]
        if max_key is None or key > max_key:
          continue
      reader = self._get_reader(shard_path)
      max_key = self._shard_max_keys.get(shard_path)
      if max_key is None or key > max_key:
        continue
      val = reader.get(key)
      if val is not None:
        yield (key, val)
        return
    yield (key, self._default_value)

  def teardown(self) -> None:
    for reader, file_obj in self._readers.values():
      try:
        reader.close()
      finally:
        if file_obj is not None:
          file_obj.close()
    self._readers.clear()


class LookupInSSTable(PTransform):
  """A ``PTransform`` that looks up each key in ``PCollection[bytes]`` across SSTable files."""

  def __init__(
      self,
      file_pattern: str,
      default_value: Any = None,
      max_open_readers: int = 64,
      block_cache_size: int = 8 * 1024 * 1024,
      use_mmap: bool = False,
  ) -> None:
    super().__init__()
    self._dofn = _LookupInSSTableDoFn(
        file_pattern=file_pattern,
        default_value=default_value,
        max_open_readers=max_open_readers,
        block_cache_size=block_cache_size,
        use_mmap=use_mmap,
    )

  def expand(
      self, pcoll: beam.PCollection[bytes]
  ) -> beam.PCollection[tuple[bytes, Any]]:
    return pcoll | "LookupInSSTable" >> beam.ParDo(self._dofn)

