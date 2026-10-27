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

#ifndef APACHE_BEAM_SSTABLE_CSRC_BINDINGS_SORTING_BUILDER_H_
#define APACHE_BEAM_SSTABLE_CSRC_BINDINGS_SORTING_BUILDER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "xla/tsl/lib/io/table_options.h"
#include "xla/tsl/platform/env.h"

namespace beam_sstable {

// High-performance SSTable builder that accepts unsorted (key, value) pairs,
// buffers them in a contiguous C++ memory arena up to max_sorting_memory_bytes,
// spills sorted runs to temporary local SSTable files when full, and performs
// a K-way min-heap merge into the final target WritableFile on Finish().
class SortingSSTableBuilder {
 public:
  SortingSSTableBuilder(const tsl::table::Options& options,
                        tsl::WritableFile* output_file,
                        size_t max_sorting_memory_bytes = 256ULL * 1024 * 1024,
                        std::string temp_dir = "/tmp",
                        bool deduplicate_keys = false);

  ~SortingSSTableBuilder();

  SortingSSTableBuilder(const SortingSSTableBuilder&) = delete;
  SortingSSTableBuilder& operator=(const SortingSSTableBuilder&) = delete;

  // Adds an unsorted (key, value) pair. May trigger a local disk spill if the
  // in-memory arena exceeds max_sorting_memory_bytes.
  absl::Status Add(absl::string_view key, absl::string_view value);

  // Sorts any remaining buffered records, merges all spilled runs (if any) into
  // output_file, and deletes all temporary spill files.
  absl::Status Finish();

  // Abandons the build and deletes any temporary spill files.
  void Abandon();

  uint64_t NumEntries() const { return num_entries_written_; }
  uint64_t TotalAddedEntries() const { return total_added_entries_; }
  uint64_t FileSize() const { return final_file_size_; }
  uint64_t NumSpills() const { return total_spills_; }

 private:
  struct RecordRef {
    const char* key_ptr;
    uint32_t key_len;
    const char* val_ptr;
    uint32_t val_len;
    uint64_t seq;

    absl::string_view key() const { return absl::string_view(key_ptr, key_len); }
    absl::string_view value() const {
      return absl::string_view(val_ptr, val_len);
    }
  };

  static constexpr size_t kDefaultArenaBlockSize = 8 * 1024 * 1024;  // 8 MiB

  char* AllocateArenaBytes(size_t n);
  void SortInMemoryRecords();
  absl::Status SpillCurrentRun();
  void ClearArena();
  void CleanupSpillFiles();

  tsl::table::Options options_;
  tsl::WritableFile* output_file_;
  size_t max_sorting_memory_bytes_;
  std::string temp_dir_;
  bool deduplicate_keys_;

  std::vector<std::unique_ptr<char[]>> arena_blocks_;
  char* current_block_ptr_ = nullptr;
  size_t current_block_remaining_ = 0;

  std::vector<RecordRef> records_;
  size_t current_memory_bytes_ = 0;
  uint64_t next_seq_ = 0;
  uint64_t total_added_entries_ = 0;
  uint64_t num_entries_written_ = 0;
  uint64_t final_file_size_ = 0;
  uint64_t total_spills_ = 0;

  std::vector<std::string> spill_files_;
  bool closed_ = false;
};

}  // namespace beam_sstable

#endif  // APACHE_BEAM_SSTABLE_CSRC_BINDINGS_SORTING_BUILDER_H_
