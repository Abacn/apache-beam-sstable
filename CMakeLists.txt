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

cmake_minimum_required(VERSION 3.24)
project(apache_beam_sstable LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

if(APPLE)
  list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew" "/usr/local")
endif()

# 1. Required external C++ dependencies
find_package(absl QUIET)
if(NOT absl_FOUND)
  include(FetchContent)
  set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
  set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    abseil-cpp
    URL https://github.com/abseil/abseil-cpp/releases/download/20250814.1/abseil-cpp-20250814.1.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  FetchContent_GetProperties(abseil-cpp)
  if(NOT abseil-cpp_POPULATED)
    FetchContent_Populate(abseil-cpp)
    add_subdirectory(${abseil-cpp_SOURCE_DIR} ${abseil-cpp_BINARY_DIR} EXCLUDE_FROM_ALL)
  endif()
endif()

find_package(Snappy QUIET)
if(NOT Snappy_FOUND)
  find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h REQUIRED)
  find_library(SNAPPY_LIBRARY NAMES snappy REQUIRED)
  add_library(Snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(Snappy::snappy PROPERTIES
    IMPORTED_LOCATION "${SNAPPY_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}"
  )
endif()

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# 2. Vendored TSL SSTable static library
add_library(tsl_sstable STATIC
  csrc/xla/tsl/lib/io/block.cc
  csrc/xla/tsl/lib/io/block_builder.cc
  csrc/xla/tsl/lib/io/cache.cc
  csrc/xla/tsl/lib/io/format.cc
  csrc/xla/tsl/lib/io/iterator.cc
  csrc/xla/tsl/lib/io/table.cc
  csrc/xla/tsl/lib/io/table_builder.cc
  csrc/xla/tsl/lib/io/two_level_iterator.cc
  csrc/xla/tsl/lib/hash/crc32c.cc
  csrc/tsl/platform/coding.cc
)

target_include_directories(tsl_sstable PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/csrc"
)

target_compile_options(tsl_sstable PUBLIC
  -Wno-deprecated-declarations
)

target_link_libraries(tsl_sstable PUBLIC
  absl::base
  absl::cord
  absl::crc32c
  absl::log
  absl::check
  absl::status
  absl::strings
  absl::synchronization
  absl::span
  Snappy::snappy
)

# 3. Pybind11 extension module _sstable_cc
pybind11_add_module(_sstable_cc MODULE
  csrc/bindings/file_adapters.cc
  csrc/bindings/table_inspector.cc
  csrc/bindings/sorting_builder.cc
  csrc/bindings/sstable_pybind.cc
)

target_include_directories(_sstable_cc PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/csrc"
)

target_link_libraries(_sstable_cc PRIVATE
  tsl_sstable
)

if(UNIX AND NOT APPLE)
  target_link_options(_sstable_cc PRIVATE "-Wl,--exclude-libs,ALL")
endif()

install(TARGETS _sstable_cc DESTINATION apache_beam_sstable)
