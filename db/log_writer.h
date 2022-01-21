// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "hyperleveldb/slice.h"
#include "hyperleveldb/status.h"
#include "port/port.h"

namespace leveldb {

class ConcurrentWritableFile;
class WritableFile;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(ConcurrentWritableFile* dest);
  ~Writer();

  Status AddRecord(const Slice& slice);

 private:
  ConcurrentWritableFile* dest_;
  uint64_t offset_; // Current offset in file

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  uint64_t ComputeRecordSize(uint64_t start, uint64_t remain);
  Status EmitPhysicalRecordAt(RecordType type, const char* ptr, uint64_t offset, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

class SimpleWriter {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this SimpleWriter is in use.
  explicit SimpleWriter(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this SimpleWriter is in use.
  SimpleWriter(WritableFile* dest, uint64_t dest_length);

  SimpleWriter(const SimpleWriter&) = delete;
  SimpleWriter& operator=(const SimpleWriter&) = delete;

  ~SimpleWriter();

  Status AddRecord(const Slice& slice);

  WritableFile* dest_;
  uint64_t dest_length_;
  int block_offset_;  // Current offset in block

 private:
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
