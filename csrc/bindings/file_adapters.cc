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

#include "bindings/file_adapters.h"

#include <errno.h>
#include <fcntl.h>
#include <snappy.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "tsl/platform/snappy.h"

namespace tsl {
namespace port {

bool Snappy_Compress(const char* input, size_t length, string* output) {
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
}

bool Snappy_CompressFromIOVec(const struct iovec* iov,
                              size_t uncompressed_length, string* output) {
  std::string flat;
  flat.resize(uncompressed_length);
  size_t offset = 0;
  for (size_t i = 0; offset < uncompressed_length; ++i) {
    size_t to_copy = std::min(iov[i].iov_len, uncompressed_length - offset);
    std::memcpy(&flat[offset], iov[i].iov_base, to_copy);
    offset += to_copy;
  }
  return Snappy_Compress(flat.data(), flat.size(), output);
}

bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                  size_t* result) {
  return snappy::GetUncompressedLength(input, length, result);
}

bool Snappy_Uncompress(const char* input, size_t length, char* output) {
  return snappy::RawUncompress(input, length, output);
}

bool Snappy_UncompressToIOVec(const char* compressed, size_t compressed_length,
                              const struct iovec* iov, size_t iov_cnt) {
  return snappy::RawUncompressToIOVec(compressed, compressed_length, iov,
                                      iov_cnt);
}

}  // namespace port
}  // namespace tsl

namespace beam_sstable {

namespace {

absl::Status PosixErrorToStatus(absl::string_view context, int err) {
  std::string msg = absl::StrCat(context, ": ", std::strerror(err));
  switch (err) {
    case ENOENT:
      return absl::NotFoundError(msg);
    case EEXIST:
      return absl::AlreadyExistsError(msg);
    case EACCES:
    case EPERM:
      return absl::PermissionDeniedError(msg);
    case EINVAL:
      return absl::InvalidArgumentError(msg);
    case ENOSPC:
    case ENOMEM:
    case EMFILE:
    case ENFILE:
      return absl::ResourceExhaustedError(msg);
    default:
      return absl::InternalError(msg);
  }
}

}  // namespace

// ============================================================================
// PosixRandomAccessFile
// ============================================================================

absl::Status PosixRandomAccessFile::Open(
    const std::string& path, std::unique_ptr<PosixRandomAccessFile>* out,
    uint64_t* file_size) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return PosixErrorToStatus(absl::StrCat("open(", path, ")"), errno);
  }
  struct stat st;
  if (::fstat(fd, &st) != 0) {
    int err = errno;
    ::close(fd);
    return PosixErrorToStatus(absl::StrCat("fstat(", path, ")"), err);
  }
  *file_size = static_cast<uint64_t>(st.st_size);
  *out = std::make_unique<PosixRandomAccessFile>(path, fd, *file_size);
  return absl::OkStatus();
}

PosixRandomAccessFile::PosixRandomAccessFile(std::string path, int fd,
                                             uint64_t file_size)
    : path_(std::move(path)), fd_(fd), file_size_(file_size) {}

PosixRandomAccessFile::~PosixRandomAccessFile() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

absl::Status PosixRandomAccessFile::Read(uint64_t offset,
                                         absl::string_view& result,
                                         absl::Span<char> scratch) const {
  char* dst = scratch.data();
  size_t left = scratch.size();
  uint64_t cur_offset = offset;

  while (left > 0) {
    ssize_t r = ::pread(fd_, dst, left, static_cast<off_t>(cur_offset));
    if (r > 0) {
      dst += r;
      left -= static_cast<size_t>(r);
      cur_offset += static_cast<uint64_t>(r);
    } else if (r == 0) {
      result = absl::string_view(scratch.data(), scratch.size() - left);
      return absl::OutOfRangeError("Read less bytes than requested (EOF)");
    } else if (errno == EINTR) {
      continue;
    } else {
      result = absl::string_view();
      return PosixErrorToStatus(absl::StrCat("pread(", path_, ")"), errno);
    }
  }

  result = absl::string_view(scratch.data(), scratch.size());
  return absl::OkStatus();
}

// ============================================================================
// MmapRandomAccessFile
// ============================================================================

absl::Status MmapRandomAccessFile::Open(
    const std::string& path, std::unique_ptr<MmapRandomAccessFile>* out,
    uint64_t* file_size) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return PosixErrorToStatus(absl::StrCat("open(", path, ")"), errno);
  }
  struct stat st;
  if (::fstat(fd, &st) != 0) {
    int err = errno;
    ::close(fd);
    return PosixErrorToStatus(absl::StrCat("fstat(", path, ")"), err);
  }
  uint64_t size = static_cast<uint64_t>(st.st_size);
  const char* base = nullptr;
  if (size > 0) {
    void* ptr = ::mmap(nullptr, static_cast<size_t>(size), PROT_READ,
                       MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
      int err = errno;
      ::close(fd);
      return PosixErrorToStatus(absl::StrCat("mmap(", path, ")"), err);
    }
    base = static_cast<const char*>(ptr);
  }
  *file_size = size;
  *out = std::make_unique<MmapRandomAccessFile>(path, fd, base, size);
  return absl::OkStatus();
}

MmapRandomAccessFile::MmapRandomAccessFile(std::string path, int fd,
                                           const char* base, uint64_t file_size)
    : path_(std::move(path)), fd_(fd), base_(base), file_size_(file_size) {}

MmapRandomAccessFile::~MmapRandomAccessFile() {
  if (base_ != nullptr && file_size_ > 0) {
    ::munmap(const_cast<char*>(base_), static_cast<size_t>(file_size_));
    base_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

absl::Status MmapRandomAccessFile::Read(uint64_t offset,
                                        absl::string_view& result,
                                        absl::Span<char> scratch) const {
  size_t n = scratch.size();
  if (offset > file_size_ || n > file_size_ - offset) {
    size_t avail = offset < file_size_ ? static_cast<size_t>(file_size_ - offset) : 0;
    result = avail > 0 ? absl::string_view(base_ + offset, avail)
                       : absl::string_view();
    return absl::OutOfRangeError("Read past end of mmap file");
  }
  // Return zero-copy view directly into the memory-mapped region.
  result = absl::string_view(base_ + offset, n);
  return absl::OkStatus();
}

// ============================================================================
// PosixBufferedWritableFile
// ============================================================================

absl::Status PosixBufferedWritableFile::Create(
    const std::string& path, std::unique_ptr<PosixBufferedWritableFile>* out,
    size_t buffer_size) {
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    return PosixErrorToStatus(absl::StrCat("open(", path, ")"), errno);
  }
  *out = std::make_unique<PosixBufferedWritableFile>(path, fd, buffer_size);
  return absl::OkStatus();
}

PosixBufferedWritableFile::PosixBufferedWritableFile(std::string path, int fd,
                                                     size_t buffer_size)
    : path_(std::move(path)),
      fd_(fd),
      buffer_capacity_(std::max<size_t>(buffer_size, 64 * 1024)) {
  buffer_.reserve(buffer_capacity_);
}

PosixBufferedWritableFile::~PosixBufferedWritableFile() {
  if (!closed_) {
    Close().IgnoreError();
  }
}

absl::Status PosixBufferedWritableFile::Append(absl::string_view data) {
  if (closed_) {
    return absl::FailedPreconditionError("WritableFile is already closed");
  }
  position_ += static_cast<int64_t>(data.size());
  if (buffer_.size() + data.size() <= buffer_capacity_) {
    buffer_.append(data.data(), data.size());
    return absl::OkStatus();
  }

  absl::Status s = FlushBuffer();
  if (!s.ok()) return s;

  if (data.size() >= buffer_capacity_) {
    return WriteRaw(data.data(), data.size());
  }
  buffer_.append(data.data(), data.size());
  return absl::OkStatus();
}

absl::Status PosixBufferedWritableFile::FlushBuffer() {
  if (buffer_.empty()) return absl::OkStatus();
  absl::Status s = WriteRaw(buffer_.data(), buffer_.size());
  buffer_.clear();
  return s;
}

absl::Status PosixBufferedWritableFile::WriteRaw(const char* data,
                                                 size_t size) {
  const char* src = data;
  size_t left = size;
  while (left > 0) {
    ssize_t written = ::write(fd_, src, left);
    if (written > 0) {
      src += written;
      left -= static_cast<size_t>(written);
    } else if (written < 0 && errno == EINTR) {
      continue;
    } else {
      return PosixErrorToStatus(absl::StrCat("write(", path_, ")"), errno);
    }
  }
  return absl::OkStatus();
}

absl::Status PosixBufferedWritableFile::Flush() {
  if (closed_) {
    return absl::FailedPreconditionError("WritableFile is already closed");
  }
  return FlushBuffer();
}

absl::Status PosixBufferedWritableFile::Sync() {
  absl::Status s = Flush();
  if (!s.ok()) return s;
  if (::fsync(fd_) != 0) {
    return PosixErrorToStatus(absl::StrCat("fsync(", path_, ")"), errno);
  }
  return absl::OkStatus();
}

absl::Status PosixBufferedWritableFile::Close() {
  if (closed_) return absl::OkStatus();
  absl::Status s = FlushBuffer();
  if (fd_ >= 0) {
    if (::close(fd_) != 0 && s.ok()) {
      s = PosixErrorToStatus(absl::StrCat("close(", path_, ")"), errno);
    }
    fd_ = -1;
  }
  closed_ = true;
  return s;
}

absl::Status PosixBufferedWritableFile::Tell(int64_t* position) {
  *position = position_;
  return absl::OkStatus();
}

// ============================================================================
// PythonRandomAccessFile
// ============================================================================

PythonRandomAccessFile::PythonRandomAccessFile(py::object py_file,
                                               uint64_t file_size)
    : py_file_(std::move(py_file)), file_size_(file_size) {}

PythonRandomAccessFile::~PythonRandomAccessFile() {
  py::gil_scoped_acquire gil;
  py_file_ = py::none();
}

absl::Status PythonRandomAccessFile::Read(uint64_t offset,
                                          absl::string_view& result,
                                          absl::Span<char> scratch) const {
  absl::MutexLock lock(&mu_);
  py::gil_scoped_acquire gil;
  try {
    py_file_.attr("seek")(offset);
    py::object data_obj = py_file_.attr("read")(scratch.size());
    py::buffer_info info = py::buffer(data_obj).request();
    size_t n_read = static_cast<size_t>(info.size);
    if (n_read > scratch.size()) {
      result = absl::string_view();
      return absl::InternalError(
          "Python file read() returned more bytes than requested");
    }
    if (n_read > 0) {
      std::memcpy(scratch.data(), info.ptr, n_read);
    }
    result = absl::string_view(scratch.data(), n_read);
    if (n_read < scratch.size()) {
      return absl::OutOfRangeError("Read less bytes than requested (EOF)");
    }
    return absl::OkStatus();
  } catch (py::error_already_set& e) {
    result = absl::string_view();
    return absl::InternalError(
        absl::StrCat("PythonRandomAccessFile::Read failed: ", e.what()));
  }
}

// ============================================================================
// PythonWritableFile
// ============================================================================

PythonWritableFile::PythonWritableFile(py::object py_file, size_t buffer_size)
    : py_file_(std::move(py_file)),
      buffer_capacity_(std::max<size_t>(buffer_size, 64 * 1024)) {
  buffer_.reserve(buffer_capacity_);
}

PythonWritableFile::~PythonWritableFile() {
  if (!closed_) {
    Close().IgnoreError();
  }
  py::gil_scoped_acquire gil;
  py_file_ = py::none();
}

absl::Status PythonWritableFile::Append(absl::string_view data) {
  if (closed_) {
    return absl::FailedPreconditionError("PythonWritableFile is already closed");
  }
  position_ += static_cast<int64_t>(data.size());
  if (buffer_.size() + data.size() <= buffer_capacity_) {
    buffer_.append(data.data(), data.size());
    return absl::OkStatus();
  }

  absl::Status s = FlushBuffer();
  if (!s.ok()) return s;

  if (data.size() >= buffer_capacity_) {
    return WriteToPython(data.data(), data.size());
  }
  buffer_.append(data.data(), data.size());
  return absl::OkStatus();
}

absl::Status PythonWritableFile::FlushBuffer() {
  if (buffer_.empty()) return absl::OkStatus();
  absl::Status s = WriteToPython(buffer_.data(), buffer_.size());
  buffer_.clear();
  return s;
}

absl::Status PythonWritableFile::WriteToPython(const char* data, size_t size) {
  if (size == 0) return absl::OkStatus();
  py::gil_scoped_acquire gil;
  try {
    py::memoryview mv = py::memoryview::from_memory(data, static_cast<py::ssize_t>(size));
    py_file_.attr("write")(mv);
    return absl::OkStatus();
  } catch (py::error_already_set& e) {
    return absl::InternalError(
        absl::StrCat("PythonWritableFile::write failed: ", e.what()));
  }
}

absl::Status PythonWritableFile::Flush() {
  if (closed_) {
    return absl::FailedPreconditionError("PythonWritableFile is already closed");
  }
  absl::Status s = FlushBuffer();
  if (!s.ok()) return s;
  py::gil_scoped_acquire gil;
  try {
    if (py::hasattr(py_file_, "flush")) {
      py_file_.attr("flush")();
    }
    return absl::OkStatus();
  } catch (py::error_already_set& e) {
    return absl::InternalError(
        absl::StrCat("PythonWritableFile::flush failed: ", e.what()));
  }
}

absl::Status PythonWritableFile::Sync() { return Flush(); }

absl::Status PythonWritableFile::Close() {
  if (closed_) return absl::OkStatus();
  absl::Status s = FlushBuffer();
  closed_ = true;
  return s;
}

absl::Status PythonWritableFile::Tell(int64_t* position) {
  *position = position_;
  return absl::OkStatus();
}

}  // namespace beam_sstable
