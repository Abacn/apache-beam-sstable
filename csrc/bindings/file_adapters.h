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

#ifndef APACHE_BEAM_SSTABLE_CSRC_BINDINGS_FILE_ADAPTERS_H_
#define APACHE_BEAM_SSTABLE_CSRC_BINDINGS_FILE_ADAPTERS_H_

#include <pybind11/pybind11.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "xla/tsl/platform/env.h"

namespace py = pybind11;

namespace beam_sstable {

// Thread-safe POSIX pread()-backed RandomAccessFile.
class PosixRandomAccessFile : public tsl::RandomAccessFile {
 public:
  static absl::Status Open(const std::string& path,
                           std::unique_ptr<PosixRandomAccessFile>* out,
                           uint64_t* file_size);

  PosixRandomAccessFile(std::string path, int fd, uint64_t file_size);
  ~PosixRandomAccessFile() override;

  absl::Status Name(absl::string_view* result) const override {
    *result = path_;
    return absl::OkStatus();
  }

  absl::Status Read(uint64_t offset, absl::string_view& result,
                    absl::Span<char> scratch) const override;

  uint64_t file_size() const { return file_size_; }

 private:
  std::string path_;
  int fd_;
  uint64_t file_size_;
};

// Zero-copy mmap()-backed RandomAccessFile for local files.
class MmapRandomAccessFile : public tsl::RandomAccessFile {
 public:
  static absl::Status Open(const std::string& path,
                           std::unique_ptr<MmapRandomAccessFile>* out,
                           uint64_t* file_size);

  MmapRandomAccessFile(std::string path, int fd, const char* base,
                       uint64_t file_size);
  ~MmapRandomAccessFile() override;

  absl::Status Name(absl::string_view* result) const override {
    *result = path_;
    return absl::OkStatus();
  }

  absl::Status Read(uint64_t offset, absl::string_view& result,
                    absl::Span<char> scratch) const override;

  uint64_t file_size() const { return file_size_; }

 private:
  std::string path_;
  int fd_;
  const char* base_;
  uint64_t file_size_;
};

// Buffered POSIX write()-backed WritableFile for local files.
class PosixBufferedWritableFile : public tsl::WritableFile {
 public:
  static constexpr size_t kDefaultBufferSize = 4 * 1024 * 1024;  // 4 MiB

  static absl::Status Create(
      const std::string& path,
      std::unique_ptr<PosixBufferedWritableFile>* out,
      size_t buffer_size = kDefaultBufferSize);

  PosixBufferedWritableFile(std::string path, int fd, size_t buffer_size);
  ~PosixBufferedWritableFile() override;

  absl::Status Append(absl::string_view data) override;
  absl::Status Close() override;
  absl::Status Flush() override;
  absl::Status Sync() override;
  absl::Status Tell(int64_t* position) override;

  absl::Status Name(absl::string_view* result) const override {
    *result = path_;
    return absl::OkStatus();
  }

 private:
  absl::Status FlushBuffer();
  absl::Status WriteRaw(const char* data, size_t size);

  std::string path_;
  int fd_;
  size_t buffer_capacity_;
  std::string buffer_;
  int64_t position_ = 0;
  bool closed_ = false;
};

// Adapter wrapping a Python seekable file-like object (e.g., Beam FileSystems.open()).
class PythonRandomAccessFile : public tsl::RandomAccessFile {
 public:
  PythonRandomAccessFile(py::object py_file, uint64_t file_size);
  ~PythonRandomAccessFile() override;

  absl::Status Read(uint64_t offset, absl::string_view& result,
                    absl::Span<char> scratch) const override;

  uint64_t file_size() const { return file_size_; }

 private:
  mutable absl::Mutex mu_;
  py::object py_file_;
  uint64_t file_size_;
};

// Adapter wrapping a Python writable file-like object (e.g., Beam FileSystems.create())
// with a large C++ write buffer to minimize GIL acquisitions and Python write calls.
class PythonWritableFile : public tsl::WritableFile {
 public:
  static constexpr size_t kDefaultBufferSize = 4 * 1024 * 1024;  // 4 MiB

  explicit PythonWritableFile(py::object py_file,
                              size_t buffer_size = kDefaultBufferSize);
  ~PythonWritableFile() override;

  absl::Status Append(absl::string_view data) override;
  absl::Status Close() override;
  absl::Status Flush() override;
  absl::Status Sync() override;
  absl::Status Tell(int64_t* position) override;

 private:
  absl::Status FlushBuffer();
  absl::Status WriteToPython(const char* data, size_t size);

  py::object py_file_;
  size_t buffer_capacity_;
  std::string buffer_;
  int64_t position_ = 0;
  bool closed_ = false;
};

}  // namespace beam_sstable

#endif  // APACHE_BEAM_SSTABLE_CSRC_BINDINGS_FILE_ADAPTERS_H_
