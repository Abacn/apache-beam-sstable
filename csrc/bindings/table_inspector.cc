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

#include "bindings/table_inspector.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "xla/tsl/lib/io/block.h"
#include "xla/tsl/lib/io/format.h"
#include "xla/tsl/lib/io/iterator.h"

namespace beam_sstable {

absl::Status InspectTableBlocks(tsl::RandomAccessFile* file,
                                uint64_t file_size,
                                std::vector<SSTableBlockEntry>* out_blocks) {
  out_blocks->clear();
  if (file_size < tsl::table::Footer::kEncodedLength) {
    return absl::DataLossError("File is too short to be an sstable");
  }

  char footer_space[tsl::table::Footer::kEncodedLength];
  absl::string_view footer_input;
  absl::Status s = file->Read(
      file_size - tsl::table::Footer::kEncodedLength, footer_input,
      absl::MakeSpan(footer_space, tsl::table::Footer::kEncodedLength));
  if (!s.ok()) return s;

  tsl::table::Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  tsl::table::BlockContents index_contents;
  s = tsl::table::ReadBlock(file, footer.index_handle(), &index_contents);
  if (!s.ok()) return s;

  tsl::table::Block index_block(index_contents);
  std::unique_ptr<tsl::table::Iterator> iter(index_block.NewIterator());
  iter->SeekToFirst();
  while (iter->Valid()) {
    tsl::table::BlockHandle handle;
    absl::string_view input = iter->value();
    s = handle.DecodeFrom(&input);
    if (!s.ok()) return s;

    SSTableBlockEntry entry;
    entry.separator_key = std::string(iter->key());
    entry.block_offset = handle.offset();
    entry.block_size = handle.size();
    entry.total_block_size = handle.size() + tsl::table::kBlockTrailerSize;
    out_blocks->push_back(std::move(entry));

    iter->Next();
  }
  return iter->status();
}

}  // namespace beam_sstable
