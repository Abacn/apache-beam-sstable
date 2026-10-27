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

"""High-performance Apache Beam Python SDK SSTable IO connector."""

from __future__ import annotations

from apache_beam_sstable.sstable import (
    TABLE_MAGIC_NUMBER,
    CompressionType,
    SortingSSTableBuilder,
    SSTableBlockEntry,
    SSTableBuilder,
    SSTableIterator,
    SSTableReader,
    crc32c_mask,
    crc32c_value,
    inspect_table_blocks,
)

__all__ = [
    "TABLE_MAGIC_NUMBER",
    "CompressionType",
    "SSTableBlockEntry",
    "SSTableBuilder",
    "SSTableIterator",
    "SSTableReader",
    "SortingSSTableBuilder",
    "crc32c_mask",
    "crc32c_value",
    "inspect_table_blocks",
    "LookupInSSTable",
    "ReadAllFromSSTable",
    "ReadFromSSTable",
    "WriteToSSTable",
]

_BEAM_TRANSFORMS = {
    "LookupInSSTable",
    "ReadAllFromSSTable",
    "ReadFromSSTable",
    "WriteToSSTable",
}


def __getattr__(name: str):
  if name in _BEAM_TRANSFORMS:
    from apache_beam_sstable import sstableio

    return getattr(sstableio, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

