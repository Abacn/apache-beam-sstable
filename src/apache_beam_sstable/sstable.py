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

"""Low-level Python API for reading and writing SSTable files."""

from __future__ import annotations

import enum
import os
import tempfile
from typing import Any, BinaryIO, Iterable, Iterator, Optional, Union

from apache_beam_sstable import _sstable_cc

FileOrPath = Union[str, os.PathLike, BinaryIO]

TABLE_MAGIC_NUMBER: int = _sstable_cc.TABLE_MAGIC_NUMBER
SSTableBlockEntry = _sstable_cc.SSTableBlockEntry
SSTableIterator = _sstable_cc.SSTableIterator


class CompressionType(enum.IntEnum):
  """Compression codec for SSTable data blocks."""

  NONE = int(_sstable_cc.COMPRESSION_NONE)
  SNAPPY = int(_sstable_cc.COMPRESSION_SNAPPY)

  # Aliases matching TensorFlow / LevelDB naming conventions
  NO_COMPRESSION = int(_sstable_cc.COMPRESSION_NONE)
  SNAPPY_COMPRESSION = int(_sstable_cc.COMPRESSION_SNAPPY)


def crc32c_value(data: bytes) -> int:
  """Computes the CRC32C checksum of ``data``."""
  return _sstable_cc.crc32c_value(data)


def crc32c_mask(crc: int) -> int:
  """Returns the masked CRC32C value as stored in SSTable block trailers."""
  return _sstable_cc.crc32c_mask(crc)


def inspect_table_blocks(
    file_or_path: FileOrPath,
    file_size: Optional[int] = None,
    use_mmap: bool = False,
) -> list[SSTableBlockEntry]:
  """Reads only the SSTable Footer and Index Block to return block boundaries."""
  return _sstable_cc.inspect_table_blocks(
      file_or_path, file_size=file_size, use_mmap=use_mmap
  )


class SSTableReader:
  """High-performance SSTable reader backed by C++ ``tsl::table::Table``.

  Supports point lookups (``reader[key]``, ``reader.get(key)``, ``key in reader``),
  full scans (``for k, v in reader``), prefix scans (``reader.iter_prefix(...)``),
  half-open range scans (``reader.iter_range(start_key, end_key)``), and block-level
  inspection/reading for splittable Apache Beam sources.
  """

  def __init__(
      self,
      file_or_path: FileOrPath,
      file_size: Optional[int] = None,
      use_mmap: bool = False,
      block_cache_size: int = 8 * 1024 * 1024,
  ) -> None:
    self._cc = _sstable_cc.SSTableReader(
        file_or_path,
        file_size=file_size,
        use_mmap=use_mmap,
        block_cache_size=block_cache_size,
    )

  @property
  def file_size(self) -> int:
    """Total byte size of the SSTable file."""
    return self._cc.file_size

  def get(self, key: bytes, default: Any = None) -> Any:
    """Point lookup for ``key``. Returns ``default`` if ``key`` is not present."""
    val = self._cc.get(key)
    return default if val is None else val

  def contains(self, key: bytes) -> bool:
    """Returns True if ``key`` exists in the SSTable."""
    return self._cc.contains(key)

  def __contains__(self, key: bytes) -> bool:
    return self._cc.contains(key)

  def __getitem__(self, key: bytes) -> bytes:
    return self._cc[key]

  def approximate_offset_of(self, key: bytes) -> int:
    """Returns the approximate byte offset of ``key`` in the SSTable file."""
    return self._cc.approximate_offset_of(key)

  def block_entries(self) -> list[SSTableBlockEntry]:
    """Returns the index entries for all data blocks in the SSTable."""
    return self._cc.block_entries()

  def read_block_records(
      self,
      block_offset: int,
      block_size: int,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      key_prefix: Optional[bytes] = None,
      keys_only: bool = False,
  ) -> list[Any]:
    """Reads and decodes all matching records from a single SSTable data block."""
    return self._cc.read_block_records(
        block_offset=block_offset,
        block_size=block_size,
        start_key=start_key,
        end_key=end_key,
        key_prefix=key_prefix,
        keys_only=keys_only,
    )

  def iter_all(self, keys_only: bool = False) -> SSTableIterator:
    """Iterates over all entries in sorted key order."""
    return self._cc.iter_all(keys_only=keys_only)

  def iter_prefix(
      self, prefix: bytes, keys_only: bool = False
  ) -> SSTableIterator:
    """Iterates over all entries whose key starts with ``prefix``."""
    return self._cc.iter_prefix(prefix=prefix, keys_only=keys_only)

  def iter_range(
      self,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      keys_only: bool = False,
  ) -> SSTableIterator:
    """Iterates over all entries in ``[start_key, end_key)``."""
    return self._cc.iter_range(
        start_key=start_key, end_key=end_key, keys_only=keys_only
    )

  def raw_iterator(
      self,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      key_prefix: Optional[bytes] = None,
      keys_only: bool = False,
  ) -> SSTableIterator:
    """Returns a low-level C++ ``SSTableIterator``."""
    return self._cc.raw_iterator(
        start_key=start_key,
        end_key=end_key,
        key_prefix=key_prefix,
        keys_only=keys_only,
    )

  def items(
      self,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      key_prefix: Optional[bytes] = None,
  ) -> SSTableIterator:
    """Iterates over ``(key, value)`` pairs matching the optional range/prefix."""
    return self._cc.raw_iterator(
        start_key=start_key,
        end_key=end_key,
        key_prefix=key_prefix,
        keys_only=False,
    )

  def keys(
      self,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      key_prefix: Optional[bytes] = None,
  ) -> SSTableIterator:
    """Iterates over keys matching the optional range/prefix without copying values."""
    return self._cc.raw_iterator(
        start_key=start_key,
        end_key=end_key,
        key_prefix=key_prefix,
        keys_only=True,
    )

  def values(
      self,
      start_key: Optional[bytes] = None,
      end_key: Optional[bytes] = None,
      key_prefix: Optional[bytes] = None,
  ) -> Iterator[bytes]:
    """Iterates over values matching the optional range/prefix."""
    for _, v in self.items(
        start_key=start_key, end_key=end_key, key_prefix=key_prefix
    ):
      yield v

  def __iter__(self) -> SSTableIterator:
    return self._cc.iter_all(keys_only=False)

  def close(self) -> None:
    """Closes the underlying SSTable and releases file descriptors/mappings."""
    self._cc.close()

  def __enter__(self) -> SSTableReader:
    return self

  def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    self.close()


class SSTableBuilder:
  """Low-level SSTable writer requiring keys to be added in non-decreasing order."""

  def __init__(
      self,
      file_or_path: FileOrPath,
      block_size: int = 262144,
      block_restart_interval: int = 16,
      compression: Union[CompressionType, int] = CompressionType.SNAPPY,
      write_buffer_size: int = 4 * 1024 * 1024,
  ) -> None:
    self._cc = _sstable_cc.SSTableBuilder(
        file_or_path,
        block_size=block_size,
        block_restart_interval=block_restart_interval,
        compression=int(compression),
        write_buffer_size=write_buffer_size,
    )

  def add(self, key: bytes, value: bytes) -> None:
    """Appends a ``(key, value)`` pair. Keys must be added in sorted order."""
    self._cc.add(key, value)

  def flush(self) -> None:
    """Flushes any buffered records into a compressed data block."""
    self._cc.flush()

  def finish(self) -> None:
    """Writes the index block and 48-byte footer, and flushes the output file."""
    self._cc.finish()

  def abandon(self) -> None:
    """Abandons the build without writing the footer."""
    self._cc.abandon()

  @property
  def num_entries(self) -> int:
    """Number of entries added so far."""
    return self._cc.num_entries

  @property
  def file_size(self) -> int:
    """Current byte size of the written SSTable."""
    return self._cc.file_size

  def __enter__(self) -> SSTableBuilder:
    return self

  def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    if exc_type is None:
      self.finish()
    else:
      self.abandon()


class SortingSSTableBuilder:
  """Out-of-core sorting SSTable writer backed by C++ arena & K-way merge.

  Accepts keys in arbitrary (unsorted) order, buffers and sorts them in native
  C++ memory up to ``max_sorting_memory_bytes``, spills intermediate sorted runs
  to local disk in ``temp_dir`` when the memory budget is exceeded, and performs
  a K-way min-heap merge into the final SSTable on ``finish()``.
  """

  def __init__(
      self,
      file_or_path: FileOrPath,
      block_size: int = 262144,
      block_restart_interval: int = 16,
      compression: Union[CompressionType, int] = CompressionType.SNAPPY,
      max_sorting_memory_bytes: int = 256 * 1024 * 1024,
      temp_dir: Optional[str] = None,
      deduplicate_keys: bool = False,
      write_buffer_size: int = 4 * 1024 * 1024,
  ) -> None:
    resolved_temp_dir = temp_dir if temp_dir is not None else tempfile.gettempdir()
    self._cc = _sstable_cc.SortingSSTableBuilder(
        file_or_path,
        block_size=block_size,
        block_restart_interval=block_restart_interval,
        compression=int(compression),
        max_sorting_memory_bytes=max_sorting_memory_bytes,
        temp_dir=resolved_temp_dir,
        deduplicate_keys=deduplicate_keys,
        write_buffer_size=write_buffer_size,
    )

  def add(self, key: bytes, value: bytes) -> None:
    """Adds an unsorted ``(key, value)`` pair to the builder."""
    self._cc.add(key, value)

  def add_batch(self, items: Iterable[tuple[bytes, bytes]]) -> None:
    """Adds a batch of ``(key, value)`` pairs."""
    if isinstance(items, (list, tuple)):
      self._cc.add_batch(items)
    else:
      for k, v in items:
        self._cc.add(k, v)

  def finish(self) -> None:
    """Sorts/merges all added records and finalizes the output SSTable."""
    self._cc.finish()

  def abandon(self) -> None:
    """Abandons the build and deletes any temporary spill files."""
    self._cc.abandon()

  @property
  def num_entries(self) -> int:
    """Number of entries written to the finalized SSTable."""
    return self._cc.num_entries

  @property
  def total_added_entries(self) -> int:
    """Total number of entries passed to ``add()`` before deduplication."""
    return self._cc.total_added_entries

  @property
  def file_size(self) -> int:
    """Final byte size of the SSTable after ``finish()``."""
    return self._cc.file_size

  @property
  def num_spills(self) -> int:
    """Number of intermediate local spill files created during sorting."""
    return self._cc.num_spills

  def __enter__(self) -> SortingSSTableBuilder:
    return self

  def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
    if exc_type is None:
      self.finish()
    else:
      self.abandon()
