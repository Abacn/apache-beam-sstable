/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef APACHE_BEAM_SSTABLE_CSRC_BINDINGS_TABLE_INSPECTOR_H_
#define APACHE_BEAM_SSTABLE_CSRC_BINDINGS_TABLE_INSPECTOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "xla/tsl/platform/env.h"

namespace beam_sstable {

// Metadata for a single data block extracted from an SSTable's index block.
struct SSTableBlockEntry {
  // Shortest separator key >= all keys in this block and < all keys in next block.
  std::string separator_key;
  // Starting byte offset of the data block in the SSTable file.
  uint64_t block_offset = 0;
  // Uncompressed/compressed block payload size in bytes (excluding 5-byte trailer).
  uint64_t block_size = 0;
  // Total on-disk bytes occupied by this block (block_size + 5-byte trailer).
  uint64_t total_block_size = 0;
};

// Reads only the 48-byte Footer and Index Block of an SSTable to extract
// all data block offsets, sizes, and separator keys without reading data blocks.
absl::Status InspectTableBlocks(tsl::RandomAccessFile* file,
                                uint64_t file_size,
                                std::vector<SSTableBlockEntry>* out_blocks);

}  // namespace beam_sstable

#endif  // APACHE_BEAM_SSTABLE_CSRC_BINDINGS_TABLE_INSPECTOR_H_
