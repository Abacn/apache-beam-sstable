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

#include "bindings/sorting_builder.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bindings/file_adapters.h"
#include "xla/tsl/lib/io/iterator.h"
#include "xla/tsl/lib/io/table.h"
#include "xla/tsl/lib/io/table_builder.h"
#include "xla/tsl/lib/io/table_options.h"

namespace beam_sstable {

namespace {

std::atomic<uint64_t> g_spill_counter{0};

// Cursor abstracting either a spilled SSTable Iterator or the final in-memory sorted run.
class MergeCursor {
 public:
  virtual ~MergeCursor() = default;
  virtual bool Valid() const = 0;
  virtual void Next() = 0;
  virtual absl::string_view key() const = 0;
  virtual absl::string_view value() const = 0;
  virtual absl::Status status() const = 0;
  virtual size_t run_index() const = 0;
};

class TableIterCursor : public MergeCursor {
 public:
  TableIterCursor(std::unique_ptr<PosixRandomAccessFile> file,
                  std::unique_ptr<tsl::table::Table> table,
                  std::unique_ptr<tsl::table::Iterator> iter, size_t run_index)
      : file_(std::move(file)),
        table_(std::move(table)),
        iter_(std::move(iter)),
        run_index_(run_index) {}

  bool Valid() const override { return iter_->Valid(); }
  void Next() override { iter_->Next(); }
  absl::string_view key() const override { return iter_->key(); }
  absl::string_view value() const override { return iter_->value(); }
  absl::Status status() const override { return iter_->status(); }
  size_t run_index() const override { return run_index_; }

 private:
  std::unique_ptr<PosixRandomAccessFile> file_;
  std::unique_ptr<tsl::table::Table> table_;
  std::unique_ptr<tsl::table::Iterator> iter_;
  size_t run_index_;
};

template <typename RecordVec>
class MemoryRunCursor : public MergeCursor {
 public:
  MemoryRunCursor(const RecordVec* records, size_t run_index)
      : records_(records), index_(0), run_index_(run_index) {}

  bool Valid() const override { return index_ < records_->size(); }
  void Next() override { ++index_; }
  absl::string_view key() const override { return (*records_)[index_].key(); }
  absl::string_view value() const override {
    return (*records_)[index_].value();
  }
  absl::Status status() const override { return absl::OkStatus(); }
  size_t run_index() const override { return run_index_; }

 private:
  const RecordVec* records_;
  size_t index_;
  size_t run_index_;
};

struct MergeCursorGreater {
  bool operator()(const MergeCursor* a, const MergeCursor* b) const {
    int cmp = a->key().compare(b->key());
    if (cmp != 0) {
      return cmp > 0;
    }
    // Earlier run_index has older records; order smaller run_index first.
    return a->run_index() > b->run_index();
  }
};

}  // namespace

SortingSSTableBuilder::SortingSSTableBuilder(
    const tsl::table::Options& options, tsl::WritableFile* output_file,
    size_t max_sorting_memory_bytes, std::string temp_dir,
    bool deduplicate_keys)
    : options_(options),
      output_file_(output_file),
      max_sorting_memory_bytes_(
          std::max<size_t>(max_sorting_memory_bytes, 64 * 1024)),
      temp_dir_(temp_dir.empty() ? "/tmp" : std::move(temp_dir)),
      deduplicate_keys_(deduplicate_keys) {}

SortingSSTableBuilder::~SortingSSTableBuilder() {
  if (!closed_) {
    Abandon();
  }
}

char* SortingSSTableBuilder::AllocateArenaBytes(size_t n) {
  if (n > current_block_remaining_) {
    size_t block_size = std::max<size_t>(kDefaultArenaBlockSize, n);
    arena_blocks_.push_back(std::make_unique<char[]>(block_size));
    current_block_ptr_ = arena_blocks_.back().get();
    current_block_remaining_ = block_size;
  }
  char* result = current_block_ptr_;
  current_block_ptr_ += n;
  current_block_remaining_ -= n;
  return result;
}

absl::Status SortingSSTableBuilder::Add(absl::string_view key,
                                        absl::string_view value) {
  if (closed_) {
    return absl::FailedPreconditionError(
        "SortingSSTableBuilder is already closed");
  }
  if (key.size() > std::numeric_limits<uint32_t>::max() ||
      value.size() > std::numeric_limits<uint32_t>::max()) {
    return absl::InvalidArgumentError(
        "Key or value exceeds 4 GiB limit in SortingSSTableBuilder");
  }

  size_t entry_bytes = key.size() + value.size() + sizeof(RecordRef);
  if (!records_.empty() &&
      current_memory_bytes_ + entry_bytes > max_sorting_memory_bytes_) {
    absl::Status s = SpillCurrentRun();
    if (!s.ok()) return s;
  }

  char* buf = AllocateArenaBytes(key.size() + value.size());
  if (!key.empty()) {
    std::memcpy(buf, key.data(), key.size());
  }
  char* val_buf = buf + key.size();
  if (!value.empty()) {
    std::memcpy(val_buf, value.data(), value.size());
  }

  records_.push_back(RecordRef{
      buf,
      static_cast<uint32_t>(key.size()),
      val_buf,
      static_cast<uint32_t>(value.size()),
      next_seq_++,
  });
  current_memory_bytes_ += entry_bytes;
  ++total_added_entries_;
  return absl::OkStatus();
}

void SortingSSTableBuilder::SortInMemoryRecords() {
  std::sort(records_.begin(), records_.end(),
            [](const RecordRef& a, const RecordRef& b) {
              int cmp = a.key().compare(b.key());
              if (cmp != 0) return cmp < 0;
              return a.seq < b.seq;
            });
}

void SortingSSTableBuilder::ClearArena() {
  records_.clear();
  records_.shrink_to_fit();
  arena_blocks_.clear();
  current_block_ptr_ = nullptr;
  current_block_remaining_ = 0;
  current_memory_bytes_ = 0;
}

void SortingSSTableBuilder::CleanupSpillFiles() {
  for (const std::string& path : spill_files_) {
    ::unlink(path.c_str());
  }
  spill_files_.clear();
}

absl::Status SortingSSTableBuilder::SpillCurrentRun() {
  if (records_.empty()) return absl::OkStatus();

  SortInMemoryRecords();

  uint64_t spill_id = g_spill_counter.fetch_add(1, std::memory_order_relaxed);
  std::string spill_path = absl::StrCat(
      temp_dir_, "/beam_sstable_spill_", ::getpid(), "_", spill_id, ".sst");

  std::unique_ptr<PosixBufferedWritableFile> spill_file;
  absl::Status s = PosixBufferedWritableFile::Create(spill_path, &spill_file);
  if (!s.ok()) return s;

  // Use uncompressed blocks for local temporary spill files to maximize CPU throughput.
  tsl::table::Options spill_options = options_;
  spill_options.compression = tsl::table::kNoCompression;

  tsl::table::TableBuilder builder(spill_options, spill_file.get());
  for (size_t i = 0; i < records_.size(); ++i) {
    if (deduplicate_keys_ && i + 1 < records_.size() &&
        records_[i].key() == records_[i + 1].key()) {
      // Keep the latest record for duplicate keys within the run.
      continue;
    }
    builder.Add(records_[i].key(), records_[i].value());
    if (!builder.status().ok()) {
      builder.Abandon();
      spill_file->Close().IgnoreError();
      ::unlink(spill_path.c_str());
      return builder.status();
    }
  }

  s = builder.Finish();
  if (!s.ok()) {
    spill_file->Close().IgnoreError();
    ::unlink(spill_path.c_str());
    return s;
  }
  s = spill_file->Close();
  if (!s.ok()) {
    ::unlink(spill_path.c_str());
    return s;
  }

  spill_files_.push_back(std::move(spill_path));
  ++total_spills_;
  ClearArena();
  return absl::OkStatus();
}

absl::Status SortingSSTableBuilder::Finish() {
  if (closed_) {
    return absl::FailedPreconditionError(
        "SortingSSTableBuilder is already closed");
  }
  closed_ = true;

  // Fast path: all records fit in memory without any disk spills.
  if (spill_files_.empty()) {
    SortInMemoryRecords();
    tsl::table::TableBuilder builder(options_, output_file_);
    for (size_t i = 0; i < records_.size(); ++i) {
      if (deduplicate_keys_ && i + 1 < records_.size() &&
          records_[i].key() == records_[i + 1].key()) {
        continue;
      }
      builder.Add(records_[i].key(), records_[i].value());
      if (!builder.status().ok()) {
        builder.Abandon();
        ClearArena();
        return builder.status();
      }
    }
    absl::Status s = builder.Finish();
    num_entries_written_ = builder.NumEntries();
    final_file_size_ = builder.FileSize();
    ClearArena();
    if (!s.ok()) return s;
    return output_file_->Flush();
  }

  // External K-way merge path: merge all spilled SSTable runs + remaining in-memory run.
  SortInMemoryRecords();

  std::vector<std::unique_ptr<MergeCursor>> cursors;
  cursors.reserve(spill_files_.size() + 1);

  tsl::table::Options read_options;
  for (size_t i = 0; i < spill_files_.size(); ++i) {
    std::unique_ptr<PosixRandomAccessFile> raf;
    uint64_t file_size = 0;
    absl::Status s = PosixRandomAccessFile::Open(spill_files_[i], &raf, &file_size);
    if (!s.ok()) {
      ClearArena();
      CleanupSpillFiles();
      return s;
    }
    tsl::table::Table* raw_table = nullptr;
    s = tsl::table::Table::Open(read_options, raf.get(), file_size, &raw_table);
    if (!s.ok()) {
      ClearArena();
      CleanupSpillFiles();
      return s;
    }
    std::unique_ptr<tsl::table::Table> table(raw_table);
    std::unique_ptr<tsl::table::Iterator> iter(table->NewIterator());
    iter->SeekToFirst();
    if (!iter->status().ok()) {
      absl::Status iter_s = iter->status();
      ClearArena();
      CleanupSpillFiles();
      return iter_s;
    }
    cursors.push_back(std::make_unique<TableIterCursor>(
        std::move(raf), std::move(table), std::move(iter), i));
  }

  if (!records_.empty()) {
    cursors.push_back(
        std::make_unique<MemoryRunCursor<std::vector<RecordRef>>>(
            &records_, spill_files_.size()));
  }

  std::priority_queue<MergeCursor*, std::vector<MergeCursor*>, MergeCursorGreater>
      min_heap;
  for (const auto& cursor : cursors) {
    if (cursor->Valid()) {
      min_heap.push(cursor.get());
    }
  }

  tsl::table::TableBuilder builder(options_, output_file_);
  std::string pending_key;
  std::string pending_val;
  bool has_pending = false;

  while (!min_heap.empty()) {
    MergeCursor* top = min_heap.top();
    min_heap.pop();

    absl::string_view k = top->key();
    absl::string_view v = top->value();

    if (deduplicate_keys_) {
      if (has_pending && k != pending_key) {
        builder.Add(pending_key, pending_val);
      }
      pending_key.assign(k.data(), k.size());
      pending_val.assign(v.data(), v.size());
      has_pending = true;
    } else {
      builder.Add(k, v);
    }

    if (!builder.status().ok()) {
      builder.Abandon();
      cursors.clear();
      ClearArena();
      CleanupSpillFiles();
      return builder.status();
    }

    top->Next();
    if (!top->status().ok()) {
      absl::Status top_s = top->status();
      builder.Abandon();
      cursors.clear();
      ClearArena();
      CleanupSpillFiles();
      return top_s;
    }
    if (top->Valid()) {
      min_heap.push(top);
    }
  }

  if (deduplicate_keys_ && has_pending) {
    builder.Add(pending_key, pending_val);
  }

  absl::Status finish_s = builder.Finish();
  num_entries_written_ = builder.NumEntries();
  final_file_size_ = builder.FileSize();

  cursors.clear();
  ClearArena();
  CleanupSpillFiles();

  if (!finish_s.ok()) return finish_s;
  return output_file_->Flush();
}

void SortingSSTableBuilder::Abandon() {
  if (closed_) return;
  closed_ = true;
  ClearArena();
  CleanupSpillFiles();
}

}  // namespace beam_sstable
