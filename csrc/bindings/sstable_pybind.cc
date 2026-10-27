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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "bindings/file_adapters.h"
#include "bindings/sorting_builder.h"
#include "bindings/table_inspector.h"
#include "xla/tsl/lib/hash/crc32c.h"
#include "xla/tsl/lib/io/block.h"
#include "xla/tsl/lib/io/cache.h"
#include "xla/tsl/lib/io/format.h"
#include "xla/tsl/lib/io/iterator.h"
#include "xla/tsl/lib/io/table.h"
#include "xla/tsl/lib/io/table_builder.h"
#include "xla/tsl/lib/io/table_options.h"

namespace py = pybind11;

namespace beam_sstable {
namespace {

void ThrowIfError(const absl::Status& s) {
  if (s.ok()) return;
  std::string msg(s.message());
  switch (s.code()) {
    case absl::StatusCode::kNotFound:
      throw py::key_error(msg);
    case absl::StatusCode::kInvalidArgument:
    case absl::StatusCode::kOutOfRange:
      throw py::value_error(msg);
    case absl::StatusCode::kDataLoss:
    case absl::StatusCode::kUnavailable:
    case absl::StatusCode::kPermissionDenied:
      PyErr_SetString(PyExc_OSError, s.ToString().c_str());
      throw py::error_already_set();
    default:
      throw std::runtime_error(absl::StrCat("[", s.ToString(), "]"));
  }
}

absl::string_view PyBufferToStringView(const py::buffer_info& info) {
  if (info.ndim != 1 || info.itemsize != 1) {
    throw py::value_error("Expected a 1-D byte buffer (bytes/bytearray/memoryview)");
  }
  return absl::string_view(static_cast<const char*>(info.ptr),
                           static_cast<size_t>(info.size));
}

tsl::table::Options MakeTableOptions(int block_size, int block_restart_interval,
                                     int compression,
                                     tsl::table::Cache* block_cache = nullptr) {
  if (block_size <= 0) {
    throw py::value_error("block_size must be > 0");
  }
  if (block_restart_interval <= 0) {
    throw py::value_error("block_restart_interval must be > 0");
  }
  if (compression != tsl::table::kNoCompression &&
      compression != tsl::table::kSnappyCompression) {
    throw py::value_error(
        "compression must be COMPRESSION_NONE (0) or COMPRESSION_SNAPPY (1)");
  }
  tsl::table::Options opts;
  opts.block_size = static_cast<size_t>(block_size);
  opts.block_restart_interval = block_restart_interval;
  opts.compression = static_cast<tsl::table::CompressionType>(compression);
  opts.block_cache = block_cache;
  return opts;
}

class PySSTableIterator {
 public:
  PySSTableIterator(std::shared_ptr<void> owner_keepalive,
                    std::unique_ptr<tsl::table::Iterator> iter,
                    std::optional<std::string> end_key = std::nullopt,
                    std::optional<std::string> key_prefix = std::nullopt,
                    bool keys_only = false)
      : owner_keepalive_(std::move(owner_keepalive)),
        iter_(std::move(iter)),
        end_key_(std::move(end_key)),
        key_prefix_(std::move(key_prefix)),
        keys_only_(keys_only) {}

  void SeekToFirst() {
    {
      py::gil_scoped_release release;
      iter_->SeekToFirst();
    }
    ThrowIfError(iter_->status());
  }

  void Seek(py::buffer target_buf) {
    py::buffer_info info = target_buf.request();
    absl::string_view target = PyBufferToStringView(info);
    {
      py::gil_scoped_release release;
      iter_->Seek(target);
    }
    ThrowIfError(iter_->status());
  }

  bool Valid() const {
    if (!iter_ || !iter_->Valid()) return false;
    absl::string_view k = iter_->key();
    if (end_key_.has_value() && k >= absl::string_view(*end_key_)) {
      return false;
    }
    if (key_prefix_.has_value() &&
        !absl::StartsWith(k, absl::string_view(*key_prefix_))) {
      return false;
    }
    return true;
  }

  void Next() {
    {
      py::gil_scoped_release release;
      iter_->Next();
    }
    ThrowIfError(iter_->status());
  }

  py::bytes key() const {
    if (!Valid()) {
      throw py::stop_iteration("Iterator is not positioned at a valid entry");
    }
    absl::string_view k = iter_->key();
    return py::bytes(k.data(), k.size());
  }

  py::bytes value() const {
    if (!Valid()) {
      throw py::stop_iteration("Iterator is not positioned at a valid entry");
    }
    absl::string_view v = iter_->value();
    return py::bytes(v.data(), v.size());
  }

  py::object NextItem() {
    if (!Valid()) {
      ThrowIfError(iter_->status());
      throw py::stop_iteration();
    }
    absl::string_view k = iter_->key();
    py::object result;
    if (keys_only_) {
      result = py::bytes(k.data(), k.size());
    } else {
      absl::string_view v = iter_->value();
      result = py::make_tuple(py::bytes(k.data(), k.size()),
                              py::bytes(v.data(), v.size()));
    }
    {
      py::gil_scoped_release release;
      iter_->Next();
    }
    ThrowIfError(iter_->status());
    return result;
  }

 private:
  std::shared_ptr<void> owner_keepalive_;
  std::unique_ptr<tsl::table::Iterator> iter_;
  std::optional<std::string> end_key_;
  std::optional<std::string> key_prefix_;
  bool keys_only_;
};

class PySSTableReader : public std::enable_shared_from_this<PySSTableReader> {
 public:
  static std::shared_ptr<PySSTableReader> Open(
      py::object file_or_path, std::optional<uint64_t> file_size,
      bool use_mmap, size_t block_cache_size) {
    auto reader = std::make_shared<PySSTableReader>();
    reader->Init(std::move(file_or_path), file_size, use_mmap,
                 block_cache_size);
    return reader;
  }

  PySSTableReader() = default;

  ~PySSTableReader() { Close(); }

  void Close() {
    table_.reset();
    file_.reset();
    cache_.reset();
  }

  uint64_t file_size() const { return file_size_; }

  py::object Get(py::buffer key_buf) {
    EnsureOpen();
    py::buffer_info info = key_buf.request();
    absl::string_view target = PyBufferToStringView(info);

    bool found = false;
    std::string value;
    absl::Status s;
    {
      py::gil_scoped_release release;
      std::unique_ptr<tsl::table::Iterator> iter(table_->NewIterator());
      iter->Seek(target);
      if (iter->Valid() && iter->key() == target) {
        found = true;
        absl::string_view v = iter->value();
        value.assign(v.data(), v.size());
      }
      s = iter->status();
    }
    ThrowIfError(s);
    if (!found) {
      return py::none();
    }
    return py::bytes(value.data(), value.size());
  }

  bool Contains(py::buffer key_buf) {
    EnsureOpen();
    py::buffer_info info = key_buf.request();
    absl::string_view target = PyBufferToStringView(info);

    bool found = false;
    absl::Status s;
    {
      py::gil_scoped_release release;
      std::unique_ptr<tsl::table::Iterator> iter(table_->NewIterator());
      iter->Seek(target);
      if (iter->Valid() && iter->key() == target) {
        found = true;
      }
      s = iter->status();
    }
    ThrowIfError(s);
    return found;
  }

  uint64_t ApproximateOffsetOf(py::buffer key_buf) {
    EnsureOpen();
    py::buffer_info info = key_buf.request();
    absl::string_view target = PyBufferToStringView(info);
    uint64_t offset = 0;
    {
      py::gil_scoped_release release;
      offset = table_->ApproximateOffsetOf(target);
    }
    return offset;
  }

  std::vector<SSTableBlockEntry> BlockEntries() {
    EnsureOpen();
    if (!cached_blocks_.has_value()) {
      std::vector<SSTableBlockEntry> blocks;
      absl::Status s;
      {
        py::gil_scoped_release release;
        s = InspectTableBlocks(file_.get(), file_size_, &blocks);
      }
      ThrowIfError(s);
      cached_blocks_ = std::move(blocks);
    }
    return *cached_blocks_;
  }

  // Reads and decodes a single SSTable data block at [block_offset, block_offset + block_size + 5),
  // applying optional start_key, end_key, key_prefix, and keys_only filters in C++.
  py::list ReadBlockRecords(uint64_t block_offset, uint64_t block_size,
                            std::optional<py::bytes> start_key_obj,
                            std::optional<py::bytes> end_key_obj,
                            std::optional<py::bytes> key_prefix_obj,
                            bool keys_only) {
    EnsureOpen();
    std::optional<std::string> start_key;
    if (start_key_obj.has_value()) {
      start_key = std::string(*start_key_obj);
    }
    std::optional<std::string> end_key;
    if (end_key_obj.has_value()) {
      end_key = std::string(*end_key_obj);
    }
    std::optional<std::string> key_prefix;
    if (key_prefix_obj.has_value()) {
      key_prefix = std::string(*key_prefix_obj);
    }

    struct RawKV {
      std::string k;
      std::string v;
    };
    std::vector<RawKV> items;
    absl::Status s;
    {
      py::gil_scoped_release release;
      tsl::table::BlockHandle handle;
      handle.set_offset(block_offset);
      handle.set_size(block_size);
      tsl::table::BlockContents contents;
      s = tsl::table::ReadBlock(file_.get(), handle, &contents);
      if (s.ok()) {
        tsl::table::Block block(contents);
        std::unique_ptr<tsl::table::Iterator> iter(block.NewIterator());
        if (start_key.has_value() && key_prefix.has_value()) {
          absl::string_view seek_target =
              *start_key >= *key_prefix ? absl::string_view(*start_key)
                                        : absl::string_view(*key_prefix);
          iter->Seek(seek_target);
        } else if (start_key.has_value()) {
          iter->Seek(*start_key);
        } else if (key_prefix.has_value()) {
          iter->Seek(*key_prefix);
        } else {
          iter->SeekToFirst();
        }

        while (iter->Valid()) {
          absl::string_view k = iter->key();
          if (end_key.has_value() && k >= absl::string_view(*end_key)) {
            break;
          }
          if (key_prefix.has_value() &&
              !absl::StartsWith(k, absl::string_view(*key_prefix))) {
            break;
          }
          if (keys_only) {
            items.push_back(RawKV{std::string(k), std::string()});
          } else {
            absl::string_view v = iter->value();
            items.push_back(RawKV{std::string(k), std::string(v)});
          }
          iter->Next();
        }
        s = iter->status();
      }
    }
    ThrowIfError(s);

    py::list out(items.size());
    for (size_t i = 0; i < items.size(); ++i) {
      if (keys_only) {
        out[i] = py::bytes(items[i].k.data(), items[i].k.size());
      } else {
        out[i] = py::make_tuple(
            py::bytes(items[i].k.data(), items[i].k.size()),
            py::bytes(items[i].v.data(), items[i].v.size()));
      }
    }
    return out;
  }

  std::unique_ptr<PySSTableIterator> NewIterator(
      std::optional<py::bytes> start_key_obj,
      std::optional<py::bytes> end_key_obj,
      std::optional<py::bytes> key_prefix_obj, bool keys_only) {
    EnsureOpen();
    std::optional<std::string> start_key;
    if (start_key_obj.has_value()) {
      start_key = std::string(*start_key_obj);
    }
    std::optional<std::string> end_key;
    if (end_key_obj.has_value()) {
      end_key = std::string(*end_key_obj);
    }
    std::optional<std::string> key_prefix;
    if (key_prefix_obj.has_value()) {
      key_prefix = std::string(*key_prefix_obj);
    }

    std::unique_ptr<tsl::table::Iterator> iter;
    {
      py::gil_scoped_release release;
      iter.reset(table_->NewIterator());
      if (start_key.has_value() && key_prefix.has_value()) {
        absl::string_view seek_target =
            *start_key >= *key_prefix ? absl::string_view(*start_key)
                                      : absl::string_view(*key_prefix);
        iter->Seek(seek_target);
      } else if (start_key.has_value()) {
        iter->Seek(*start_key);
      } else if (key_prefix.has_value()) {
        iter->Seek(*key_prefix);
      } else {
        iter->SeekToFirst();
      }
    }
    ThrowIfError(iter->status());
    return std::make_unique<PySSTableIterator>(
        shared_from_this(), std::move(iter), std::move(end_key),
        std::move(key_prefix), keys_only);
  }

 private:
  void Init(py::object file_or_path, std::optional<uint64_t> file_size,
            bool use_mmap, size_t block_cache_size) {
    if (block_cache_size > 0) {
      cache_.reset(tsl::table::NewLRUCache(block_cache_size));
    }

    if (py::isinstance<py::str>(file_or_path) ||
        py::hasattr(file_or_path, "__fspath__")) {
      std::string path = py::str(
          py::hasattr(file_or_path, "__fspath__")
              ? file_or_path.attr("__fspath__")()
              : file_or_path);
      absl::Status s;
      uint64_t detected_size = 0;
      {
        py::gil_scoped_release release;
        if (use_mmap) {
          std::unique_ptr<MmapRandomAccessFile> mmap_file;
          s = MmapRandomAccessFile::Open(path, &mmap_file, &detected_size);
          if (s.ok()) file_ = std::move(mmap_file);
        } else {
          std::unique_ptr<PosixRandomAccessFile> posix_file;
          s = PosixRandomAccessFile::Open(path, &posix_file, &detected_size);
          if (s.ok()) file_ = std::move(posix_file);
        }
      }
      ThrowIfError(s);
      file_size_ = file_size.value_or(detected_size);
    } else {
      uint64_t resolved_size = 0;
      if (file_size.has_value()) {
        resolved_size = *file_size;
      } else {
        file_or_path.attr("seek")(0, 2);  // SEEK_END
        resolved_size = py::cast<uint64_t>(file_or_path.attr("tell")());
      }
      file_size_ = resolved_size;
      file_ = std::make_unique<PythonRandomAccessFile>(std::move(file_or_path),
                                                       file_size_);
    }

    tsl::table::Options opts;
    opts.block_cache = cache_.get();
    tsl::table::Table* raw_table = nullptr;
    absl::Status s;
    {
      py::gil_scoped_release release;
      s = tsl::table::Table::Open(opts, file_.get(), file_size_, &raw_table);
    }
    ThrowIfError(s);
    table_.reset(raw_table);
  }

  void EnsureOpen() const {
    if (!table_) {
      throw std::runtime_error("SSTableReader is closed");
    }
  }

  std::unique_ptr<tsl::table::Cache> cache_;
  std::unique_ptr<tsl::RandomAccessFile> file_;
  std::unique_ptr<tsl::table::Table> table_;
  uint64_t file_size_ = 0;
  std::optional<std::vector<SSTableBlockEntry>> cached_blocks_;
};

// Direct streaming SSTableBuilder (requires keys to be added in sorted order).
class PySSTableBuilder {
 public:
  PySSTableBuilder(py::object file_or_path, int block_size,
                   int block_restart_interval, int compression,
                   size_t write_buffer_size) {
    tsl::table::Options opts =
        MakeTableOptions(block_size, block_restart_interval, compression);
    if (py::isinstance<py::str>(file_or_path) ||
        py::hasattr(file_or_path, "__fspath__")) {
      std::string path = py::str(
          py::hasattr(file_or_path, "__fspath__")
              ? file_or_path.attr("__fspath__")()
              : file_or_path);
      std::unique_ptr<PosixBufferedWritableFile> posix_file;
      absl::Status s;
      {
        py::gil_scoped_release release;
        s = PosixBufferedWritableFile::Create(path, &posix_file,
                                              write_buffer_size);
      }
      ThrowIfError(s);
      file_ = std::move(posix_file);
    } else {
      file_ = std::make_unique<PythonWritableFile>(std::move(file_or_path),
                                                   write_buffer_size);
    }
    builder_ = std::make_unique<tsl::table::TableBuilder>(opts, file_.get());
  }

  ~PySSTableBuilder() {
    if (!closed_ && builder_) {
      builder_->Abandon();
      closed_ = true;
    }
  }

  void Add(py::buffer key_buf, py::buffer val_buf) {
    EnsureOpen();
    py::buffer_info k_info = key_buf.request();
    py::buffer_info v_info = val_buf.request();
    absl::string_view k = PyBufferToStringView(k_info);
    absl::string_view v = PyBufferToStringView(v_info);

    if (has_last_key_ && k <= absl::string_view(last_key_)) {
      throw py::value_error(
          "SSTableBuilder requires strictly increasing keys (got key <= last_key)");
    }
    last_key_.assign(k.data(), k.size());
    has_last_key_ = true;

    absl::Status s;
    {
      py::gil_scoped_release release;
      builder_->Add(k, v);
      s = builder_->status();
    }
    ThrowIfError(s);
  }

  void Flush() {
    EnsureOpen();
    absl::Status s;
    {
      py::gil_scoped_release release;
      builder_->Flush();
      s = builder_->status();
    }
    ThrowIfError(s);
  }

  void Finish() {
    EnsureOpen();
    closed_ = true;
    absl::Status s;
    {
      py::gil_scoped_release release;
      s = builder_->Finish();
      if (s.ok()) {
        s = file_->Close();
      }
    }
    ThrowIfError(s);
  }

  void Abandon() {
    if (closed_) return;
    closed_ = true;
    if (builder_) {
      builder_->Abandon();
    }
    if (file_) {
      file_->Close().IgnoreError();
    }
  }

  uint64_t num_entries() const {
    return builder_ ? builder_->NumEntries() : 0;
  }

  uint64_t file_size() const { return builder_ ? builder_->FileSize() : 0; }

 private:
  void EnsureOpen() const {
    if (closed_ || !builder_) {
      throw std::runtime_error("SSTableBuilder is already closed");
    }
  }

  std::unique_ptr<tsl::WritableFile> file_;
  std::unique_ptr<tsl::table::TableBuilder> builder_;
  std::string last_key_;
  bool has_last_key_ = false;
  bool closed_ = false;
};

// Unsorted SSTable builder backed by beam_sstable::SortingSSTableBuilder.
class PySortingSSTableBuilder {
 public:
  PySortingSSTableBuilder(py::object file_or_path, int block_size,
                          int block_restart_interval, int compression,
                          size_t max_sorting_memory_bytes,
                          std::string temp_dir, bool deduplicate_keys,
                          size_t write_buffer_size) {
    tsl::table::Options opts =
        MakeTableOptions(block_size, block_restart_interval, compression);
    if (py::isinstance<py::str>(file_or_path) ||
        py::hasattr(file_or_path, "__fspath__")) {
      std::string path = py::str(
          py::hasattr(file_or_path, "__fspath__")
              ? file_or_path.attr("__fspath__")()
              : file_or_path);
      std::unique_ptr<PosixBufferedWritableFile> posix_file;
      absl::Status s;
      {
        py::gil_scoped_release release;
        s = PosixBufferedWritableFile::Create(path, &posix_file,
                                              write_buffer_size);
      }
      ThrowIfError(s);
      file_ = std::move(posix_file);
    } else {
      file_ = std::make_unique<PythonWritableFile>(std::move(file_or_path),
                                                   write_buffer_size);
    }
    sorting_builder_ = std::make_unique<SortingSSTableBuilder>(
        opts, file_.get(), max_sorting_memory_bytes, std::move(temp_dir),
        deduplicate_keys);
  }

  ~PySortingSSTableBuilder() {
    if (!closed_ && sorting_builder_) {
      sorting_builder_->Abandon();
      closed_ = true;
    }
  }

  void Add(py::buffer key_buf, py::buffer val_buf) {
    EnsureOpen();
    py::buffer_info k_info = key_buf.request();
    py::buffer_info v_info = val_buf.request();
    absl::string_view k = PyBufferToStringView(k_info);
    absl::string_view v = PyBufferToStringView(v_info);

    absl::Status s;
    {
      py::gil_scoped_release release;
      s = sorting_builder_->Add(k, v);
    }
    ThrowIfError(s);
  }

  void AddBatch(py::iterable items) {
    EnsureOpen();
    for (py::handle item : items) {
      py::tuple pair = py::reinterpret_borrow<py::tuple>(item);
      if (pair.size() != 2) {
        throw py::value_error("Each item in add_batch must be a (key, value) pair");
      }
      py::buffer k_buf = py::reinterpret_borrow<py::buffer>(pair[0]);
      py::buffer v_buf = py::reinterpret_borrow<py::buffer>(pair[1]);
      Add(k_buf, v_buf);
    }
  }

  void Finish() {
    EnsureOpen();
    closed_ = true;
    absl::Status s;
    {
      py::gil_scoped_release release;
      s = sorting_builder_->Finish();
      if (s.ok()) {
        s = file_->Close();
      }
    }
    ThrowIfError(s);
  }

  void Abandon() {
    if (closed_) return;
    closed_ = true;
    if (sorting_builder_) {
      sorting_builder_->Abandon();
    }
    if (file_) {
      file_->Close().IgnoreError();
    }
  }

  uint64_t num_entries() const {
    return sorting_builder_ ? sorting_builder_->NumEntries() : 0;
  }

  uint64_t total_added_entries() const {
    return sorting_builder_ ? sorting_builder_->TotalAddedEntries() : 0;
  }

  uint64_t file_size() const {
    return sorting_builder_ ? sorting_builder_->FileSize() : 0;
  }

  uint64_t num_spills() const {
    return sorting_builder_ ? sorting_builder_->NumSpills() : 0;
  }

 private:
  void EnsureOpen() const {
    if (closed_ || !sorting_builder_) {
      throw std::runtime_error("SortingSSTableBuilder is already closed");
    }
  }

  std::unique_ptr<tsl::WritableFile> file_;
  std::unique_ptr<SortingSSTableBuilder> sorting_builder_;
  bool closed_ = false;
};

}  // namespace

PYBIND11_MODULE(_sstable_cc, m) {
  m.doc() = "C++ SSTable bindings backed by TensorFlow tsl::table";

  m.attr("COMPRESSION_NONE") = py::int_(static_cast<int>(tsl::table::kNoCompression));
  m.attr("COMPRESSION_SNAPPY") =
      py::int_(static_cast<int>(tsl::table::kSnappyCompression));
  m.attr("TABLE_MAGIC_NUMBER") = py::int_(tsl::table::kTableMagicNumber);

  m.def(
      "crc32c_value",
      [](py::buffer data_buf) -> uint32_t {
        py::buffer_info info = data_buf.request();
        absl::string_view sv = PyBufferToStringView(info);
        uint32_t crc = 0;
        {
          py::gil_scoped_release release;
          crc = tsl::crc32c::Value(sv.data(), sv.size());
        }
        return crc;
      },
      py::arg("data"),
      "Computes the CRC32C checksum of a byte buffer.");

  m.def(
      "crc32c_mask",
      [](uint32_t crc) -> uint32_t { return tsl::crc32c::Mask(crc); },
      py::arg("crc"),
      "Returns the masked representation of a CRC32C value.");

  py::class_<SSTableBlockEntry>(m, "SSTableBlockEntry")
      .def_property_readonly("separator_key",
                             [](const SSTableBlockEntry& e) {
                               return py::bytes(e.separator_key.data(),
                                                e.separator_key.size());
                             })
      .def_readonly("block_offset", &SSTableBlockEntry::block_offset)
      .def_readonly("block_size", &SSTableBlockEntry::block_size)
      .def_readonly("total_block_size", &SSTableBlockEntry::total_block_size)
      .def("__repr__", [](const SSTableBlockEntry& e) {
        return absl::StrCat("SSTableBlockEntry(block_offset=", e.block_offset,
                            ", block_size=", e.block_size,
                            ", total_block_size=", e.total_block_size, ")");
      });

  m.def(
      "inspect_table_blocks",
      [](py::object file_or_path, std::optional<uint64_t> file_size,
         bool use_mmap) {
        std::unique_ptr<tsl::RandomAccessFile> file;
        uint64_t resolved_size = 0;
        if (py::isinstance<py::str>(file_or_path) ||
            py::hasattr(file_or_path, "__fspath__")) {
          std::string path = py::str(
              py::hasattr(file_or_path, "__fspath__")
                  ? file_or_path.attr("__fspath__")()
                  : file_or_path);
          absl::Status s;
          uint64_t detected_size = 0;
          {
            py::gil_scoped_release release;
            if (use_mmap) {
              std::unique_ptr<MmapRandomAccessFile> mmap_file;
              s = MmapRandomAccessFile::Open(path, &mmap_file, &detected_size);
              if (s.ok()) file = std::move(mmap_file);
            } else {
              std::unique_ptr<PosixRandomAccessFile> posix_file;
              s = PosixRandomAccessFile::Open(path, &posix_file,
                                              &detected_size);
              if (s.ok()) file = std::move(posix_file);
            }
          }
          ThrowIfError(s);
          resolved_size = file_size.value_or(detected_size);
        } else {
          if (file_size.has_value()) {
            resolved_size = *file_size;
          } else {
            file_or_path.attr("seek")(0, 2);
            resolved_size = py::cast<uint64_t>(file_or_path.attr("tell")());
          }
          file = std::make_unique<PythonRandomAccessFile>(
              std::move(file_or_path), resolved_size);
        }

        std::vector<SSTableBlockEntry> blocks;
        absl::Status s;
        {
          py::gil_scoped_release release;
          s = InspectTableBlocks(file.get(), resolved_size, &blocks);
        }
        ThrowIfError(s);
        return blocks;
      },
      py::arg("file_or_path"), py::arg("file_size") = py::none(),
      py::arg("use_mmap") = false,
      "Reads only the SSTable Footer and Index Block to return block boundaries.");

  py::class_<PySSTableIterator>(m, "SSTableIterator")
      .def("seek_to_first", &PySSTableIterator::SeekToFirst)
      .def("seek", &PySSTableIterator::Seek, py::arg("target"))
      .def("valid", &PySSTableIterator::Valid)
      .def("next", &PySSTableIterator::Next)
      .def("key", &PySSTableIterator::key)
      .def("value", &PySSTableIterator::value)
      .def("__iter__", [](PySSTableIterator& self) -> PySSTableIterator& {
        return self;
      })
      .def("__next__", &PySSTableIterator::NextItem);

  py::class_<PySSTableReader, std::shared_ptr<PySSTableReader>>(m,
                                                                "SSTableReader")
      .def(py::init(&PySSTableReader::Open), py::arg("file_or_path"),
           py::arg("file_size") = py::none(), py::arg("use_mmap") = false,
           py::arg("block_cache_size") = 8ULL * 1024 * 1024)
      .def_property_readonly("file_size", &PySSTableReader::file_size)
      .def("get", &PySSTableReader::Get, py::arg("key"))
      .def("contains", &PySSTableReader::Contains, py::arg("key"))
      .def("__contains__", &PySSTableReader::Contains, py::arg("key"))
      .def("__getitem__",
           [](PySSTableReader& self, py::buffer key) {
             py::object val = self.Get(key);
             if (val.is_none()) {
               throw py::key_error("Key not found in SSTable");
             }
             return val;
           })
      .def("approximate_offset_of", &PySSTableReader::ApproximateOffsetOf,
           py::arg("key"))
      .def("block_entries", &PySSTableReader::BlockEntries)
      .def("read_block_records", &PySSTableReader::ReadBlockRecords,
           py::arg("block_offset"), py::arg("block_size"),
           py::arg("start_key") = py::none(), py::arg("end_key") = py::none(),
           py::arg("key_prefix") = py::none(), py::arg("keys_only") = false)
      .def(
          "iter_all",
          [](PySSTableReader& self, bool keys_only) {
            return self.NewIterator(std::nullopt, std::nullopt, std::nullopt,
                                    keys_only);
          },
          py::arg("keys_only") = false)
      .def(
          "iter_prefix",
          [](PySSTableReader& self, py::bytes prefix, bool keys_only) {
            return self.NewIterator(std::nullopt, std::nullopt,
                                    std::move(prefix), keys_only);
          },
          py::arg("prefix"), py::arg("keys_only") = false)
      .def(
          "iter_range",
          [](PySSTableReader& self, std::optional<py::bytes> start_key,
             std::optional<py::bytes> end_key, bool keys_only) {
            return self.NewIterator(std::move(start_key), std::move(end_key),
                                    std::nullopt, keys_only);
          },
          py::arg("start_key") = py::none(), py::arg("end_key") = py::none(),
          py::arg("keys_only") = false)
      .def("raw_iterator", &PySSTableReader::NewIterator,
           py::arg("start_key") = py::none(), py::arg("end_key") = py::none(),
           py::arg("key_prefix") = py::none(), py::arg("keys_only") = false)
      .def("__iter__",
           [](PySSTableReader& self) {
             return self.NewIterator(std::nullopt, std::nullopt, std::nullopt,
                                     false);
           })
      .def("close", &PySSTableReader::Close)
      .def("__enter__",
           [](std::shared_ptr<PySSTableReader> self) { return self; })
      .def("__exit__",
           [](PySSTableReader& self, const py::object&, const py::object&,
              const py::object&) { self.Close(); });

  py::class_<PySSTableBuilder>(m, "SSTableBuilder")
      .def(py::init<py::object, int, int, int, size_t>(),
           py::arg("file_or_path"), py::arg("block_size") = 262144,
           py::arg("block_restart_interval") = 16,
           py::arg("compression") =
               static_cast<int>(tsl::table::kSnappyCompression),
           py::arg("write_buffer_size") = 4ULL * 1024 * 1024)
      .def("add", &PySSTableBuilder::Add, py::arg("key"), py::arg("value"))
      .def("flush", &PySSTableBuilder::Flush)
      .def("finish", &PySSTableBuilder::Finish)
      .def("abandon", &PySSTableBuilder::Abandon)
      .def_property_readonly("num_entries", &PySSTableBuilder::num_entries)
      .def_property_readonly("file_size", &PySSTableBuilder::file_size)
      .def("__enter__", [](PySSTableBuilder& self) -> PySSTableBuilder& {
        return self;
      })
      .def("__exit__",
           [](PySSTableBuilder& self, const py::object& exc_type,
              const py::object&, const py::object&) {
             if (exc_type.is_none()) {
               self.Finish();
             } else {
               self.Abandon();
             }
           });

  py::class_<PySortingSSTableBuilder>(m, "SortingSSTableBuilder")
      .def(py::init<py::object, int, int, int, size_t, std::string, bool,
                    size_t>(),
           py::arg("file_or_path"), py::arg("block_size") = 262144,
           py::arg("block_restart_interval") = 16,
           py::arg("compression") =
               static_cast<int>(tsl::table::kSnappyCompression),
           py::arg("max_sorting_memory_bytes") = 256ULL * 1024 * 1024,
           py::arg("temp_dir") = "/tmp",
           py::arg("deduplicate_keys") = false,
           py::arg("write_buffer_size") = 4ULL * 1024 * 1024)
      .def("add", &PySortingSSTableBuilder::Add, py::arg("key"),
           py::arg("value"))
      .def("add_batch", &PySortingSSTableBuilder::AddBatch, py::arg("items"))
      .def("finish", &PySortingSSTableBuilder::Finish)
      .def("abandon", &PySortingSSTableBuilder::Abandon)
      .def_property_readonly("num_entries",
                             &PySortingSSTableBuilder::num_entries)
      .def_property_readonly("total_added_entries",
                             &PySortingSSTableBuilder::total_added_entries)
      .def_property_readonly("file_size", &PySortingSSTableBuilder::file_size)
      .def_property_readonly("num_spills",
                             &PySortingSSTableBuilder::num_spills)
      .def("__enter__",
           [](PySortingSSTableBuilder& self) -> PySortingSSTableBuilder& {
             return self;
           })
      .def("__exit__",
           [](PySortingSSTableBuilder& self, const py::object& exc_type,
              const py::object&, const py::object&) {
             if (exc_type.is_none()) {
               self.Finish();
             } else {
               self.Abandon();
             }
           });
}

}  // namespace beam_sstable
