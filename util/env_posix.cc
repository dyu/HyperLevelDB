// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "hyperleveldb/env.h"
#include "hyperleveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, strerror(error_number));
  } else {
    return Status::IOError(context, strerror(error_number));
  }
}

// Returns the directory name in a path pointing to a file.
//
// Returns "." if the path does not contain any directory separator.
std::string Dirname(const std::string& filename) {
  std::string::size_type separator_pos = filename.rfind('/');
  if (separator_pos == std::string::npos) {
    return std::string(".");
  }
  // The filename component should not contain a path separator. If it does,
  // the splitting was done incorrectly.
  assert(filename.find('/', separator_pos + 1) == std::string::npos);
  return filename.substr(0, separator_pos);
}

// Extracts the file name from a path pointing to a file.
//
// The returned Slice points to |filename|'s data buffer, so it is only valid
// while |filename| is alive and unchanged.
Slice Basename(const std::string& filename) {
  std::string::size_type separator_pos = filename.rfind('/');
  if (separator_pos == std::string::npos) {
    return Slice(filename);
  }
  // The filename component should not contain a path separator. If it does,
  // the splitting was done incorrectly.
  assert(filename.find('/', separator_pos + 1) == std::string::npos);
  return Slice(filename.data() + separator_pos + 1,
               filename.length() - separator_pos - 1);
}
 // True if the given file is a manifest file.
bool IsManifest(const std::string& filename) {
  return Basename(filename).starts_with("MANIFEST");
}

// Ensures that all the caches associated with the given file descriptor's
// data are flushed all the way to durable media, and can withstand power
// failures.
//
// The path argument is only used to populate the description string in the
// returned Status if an error occurs.
Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
  // On macOS and iOS, fsync() doesn't guarantee durability past power
  // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
  // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
  // fsync().
  if (::fcntl(fd, F_FULLFSYNC) == 0) {
    return Status::OK();
  }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
  bool sync_success = ::fdatasync(fd) == 0;
#else
  bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

  if (sync_success) {
    return Status::OK();
  }
  return PosixError(fd_path, errno);
}

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(filename) {}
  ~PosixSequentialFile() override { close(fd_); }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {  // Read error.
        if (errno == EINTR) {
          continue;  // Retry
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status Skip(uint64_t n) override {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  ~PosixRandomAccessFile() override { close(fd_); }

  Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const override {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = PosixError(filename_, errno);
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter()
    : mu_(),
      allowed_() {
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  PosixMmapReadableFile(const PosixMmapReadableFile&);
  PosixMmapReadableFile& operator = (const PosixMmapReadableFile&);
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  ~PosixMmapReadableFile() override {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
                      char* /*scratch*/) const override {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = PosixError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < kWritableFileBufferSize) {
      memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    return SyncFd(fd_, filename_);
  }

 private:
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  char buf_[kWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_;  // The directory of filename_.
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PosixMmapFile : public ConcurrentWritableFile {
 private:
  PosixMmapFile(const PosixMmapFile&);
  PosixMmapFile& operator = (const PosixMmapFile&);
  struct MmapSegment{
    char* base_;
  };

  bool is_manifest_;        // True if the file's name starts with MANIFEST.
  std::string filename_;    // Path to the file
  std::string dirname_;     // The directory of filename_.
  int fd_;                  // The open file
  const size_t block_size_; // System page size
  uint64_t end_offset_;     // Where does the file end?
  MmapSegment* segments_;   // mmap'ed regions of memory
  size_t segments_sz_;      // number of segments that are truncated
  bool trunc_in_progress_;  // is there an ongoing truncate operation?
  uint64_t trunc_waiters_;  // number of threads waiting for truncate
  port::Mutex mtx_;         // Protection for state
  port::CondVar cnd_;       // Wait for truncate

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  bool GrowViaTruncate(uint64_t block) {
    mtx_.Lock();
    while (trunc_in_progress_ && segments_sz_ <= block) {
      ++trunc_waiters_;
      cnd_.Wait();
      --trunc_waiters_;
    }
    uint64_t cur_sz = segments_sz_;
    trunc_in_progress_ = cur_sz <= block;
    mtx_.Unlock();

    bool error = false;
    if (cur_sz <= block) {
      uint64_t new_sz = ((block + 7) & ~7ULL) + 1;
      if (ftruncate(fd_, new_sz * block_size_) < 0) {
        error = true;
      }
      MmapSegment* new_segs = new MmapSegment[new_sz];
      MmapSegment* old_segs = NULL;
      mtx_.Lock();
      old_segs = segments_;
      for (size_t i = 0; i < segments_sz_; ++i) {
        new_segs[i].base_ = old_segs[i].base_;
      }
      for (size_t i = segments_sz_; i < new_sz; ++i) {
        new_segs[i].base_ = NULL;
      }
      segments_ = new_segs;
      segments_sz_ = new_sz;
      trunc_in_progress_ = false;
      cnd_.SignalAll();
      mtx_.Unlock();
      delete[] old_segs;
    }
    return !error;
  }

  bool UnmapSegment(char* base) {
    return munmap(base, block_size_) >= 0;
  }

  // Call holding mtx_
  char* GetSegment(uint64_t block) {
    char* base = NULL;
    mtx_.Lock();
    size_t cur_sz = segments_sz_;
    if (block < segments_sz_) {
      base = segments_[block].base_;
    }
    mtx_.Unlock();
    if (base) {
      return base;
    }
    if (cur_sz <= block) {
      if (!GrowViaTruncate(block)) {
        return NULL;
      }
    }
    void* ptr = mmap(NULL, block_size_, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd_, block * block_size_);
    if (ptr == MAP_FAILED) {
      abort();
      return NULL;
    }
    bool unmap = false;
    mtx_.Lock();
    assert(block < segments_sz_);
    if (segments_[block].base_) {
      base = segments_[block].base_;
      unmap = true;
    } else {
      base = reinterpret_cast<char*>(ptr);
      segments_[block].base_ = base;
      unmap = false;
    }
    mtx_.Unlock();
    if (unmap) {
      if (!UnmapSegment(reinterpret_cast<char*>(ptr))) {
        return NULL;
      }
    }
    return base;
  }

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size)
      : is_manifest_(IsManifest(fname)),
        filename_(fname),
        dirname_(Dirname(fname)),
        fd_(fd),
        block_size_(Roundup(page_size, 262144)),
        end_offset_(0),
        segments_(NULL),
        segments_sz_(0),
        trunc_in_progress_(false),
        trunc_waiters_(0),
        mtx_(),
        cnd_(&mtx_) {
    assert((page_size & (page_size - 1)) == 0);
  }

  ~PosixMmapFile() override {
    PosixMmapFile::Close();
  }

  Status WriteAt(uint64_t offset, const Slice& data) override {
    const uint64_t end = offset + data.size();
    const char* src = data.data();
    uint64_t rem = data.size();
    mtx_.Lock();
    end_offset_ = end_offset_ < end ? end : end_offset_;
    mtx_.Unlock();
    while (rem > 0) {
      const uint64_t block = offset / block_size_;
      char* base = GetSegment(block);
      if (!base) {
        return PosixError(filename_, errno);
      }
      const uint64_t loff = offset - block * block_size_;
      uint64_t n = block_size_ - loff;
      n = n < rem ? n : rem;
      memmove(base + loff, src, n);
      rem -= n;
      src += n;
      offset += n;
    }
    return Status::OK();
  }

  Status Append(const Slice& data) override {
    mtx_.Lock();
    uint64_t offset = end_offset_;
    mtx_.Unlock();
    return WriteAt(offset, data);
  }

  Status Close() override {
    Status s;
    int fd;
    MmapSegment* segments;
    size_t end_offset;
    size_t segments_sz;
    mtx_.Lock();
    fd = fd_;
    fd_ = -1;
    end_offset = end_offset_;
    end_offset_ = 0;
    segments = segments_;
    segments_ = NULL;
    segments_sz = segments_sz_;
    segments_sz_ = 0;
    mtx_.Unlock();
    if (fd < 0) {
      return s;
    }
    for (size_t i = 0; i < segments_sz; ++i) {
      if (segments[i].base_ != NULL &&
          munmap(segments[i].base_, block_size_) < 0) {
        s = PosixError(filename_, errno);
      }
    }
    delete[] segments;
    if (ftruncate(fd, end_offset) < 0) {
      s = PosixError(filename_, errno);
    }
    if (close(fd) < 0) {
      if (s.ok()) {
        s = PosixError(filename_, errno);
      }
    }
    return s;
  }

  Status Flush() override {
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    /*
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = PosixError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = PosixError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
    */
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();

    if (!s.ok()) {
      return s;
    }

    size_t block = 0;
    while (true) {
      char* base = NULL;
      mtx_.Lock();
      if (block < segments_sz_) {
        base = segments_[block].base_;
      }
      mtx_.Unlock();
      if (!base) {
        break;
      }
      if (msync(base, block_size_, MS_SYNC) < 0) {
        s = PosixError(filename_, errno);
      }
      ++block;
    }
    return s;
  }
};

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  PosixFileLock() : fd_(-1), name_() { }
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable {
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_;
 public:
  PosixLockTable() : mu_(), locked_files_() { }
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv final : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static const char msg[] =
        "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) override {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      s = PosixError(fname, errno);
    } else if (mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = PosixError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewConcurrentWritableFile(const std::string& fname,
                                           ConcurrentWritableFile** result) override {
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      *result = NULL;
      s = PosixError(fname, errno);
    } else {
      *result = new PosixMmapFile(fname, fd, page_size_);
    }
    return s;
  }

  bool FileExists(const std::string& fname) override {
    return access(fname.c_str(), F_OK) == 0;
  }

  Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return PosixError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  Status DeleteFile(const std::string& fname) override {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = PosixError(fname, errno);
    }
    return result;
  }

  Status CreateDir(const std::string& name) override {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = PosixError(name, errno);
    }
    return result;
  }

  Status DeleteDir(const std::string& name) override {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = PosixError(name, errno);
    }
    return result;
  }

   Status GetFileSize(const std::string& fname, uint64_t* size) override {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = PosixError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  Status RenameFile(const std::string& src, const std::string& target) override {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = PosixError(src, errno);
    }
    return result;
  }

  Status CopyFile(const std::string& src, const std::string& target) override {
    Status result;
    int fd1 = -1;
    int fd2 = -1;

    if (result.ok() && (fd1 = open(src.c_str(), O_RDONLY)) < 0) {
      result = PosixError(src, errno);
    }
    if (result.ok() && (fd2 = open(target.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
      result = PosixError(target, errno);
    }

    ssize_t amt = 0;
    char buf[512];

    while (result.ok() && (amt = read(fd1, buf, 512)) > 0) {
      if (write(fd2, buf, amt) != amt) {
        result = PosixError(src, errno);
      }
    }

    if (result.ok() && amt < 0) {
      result = PosixError(src, errno);
    }

    if (fd1 >= 0 && close(fd1) < 0) {
      if (result.ok()) {
        result = PosixError(src, errno);
      }
    }

    if (fd2 >= 0 && close(fd2) < 0) {
      if (result.ok()) {
        result = PosixError(target, errno);
      }
    }

    return result;
  }

  Status LinkFile(const std::string& src, const std::string& target) override {
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = PosixError(src, errno);
    }
    return result;
  }

  Status LockFile(const std::string& fname, FileLock** lock) override {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = PosixError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      result = PosixError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = PosixError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  void Schedule(void (*function)(void*), void* arg) override;

  void StartThread(void (*function)(void* arg), void* arg) override;

  Status GetTestDirectory(std::string* result) override {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  Status NewLogger(const std::string& fname, Logger** result) override {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return PosixError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  PosixLockTable locks_;
  MmapLimiter mmap_limit_;
};

PosixEnv::PosixEnv() : page_size_(getpagesize()),
                       mu_(),
                       bgsignal_(),
                       bgthread_(),
                       started_bgthread_(false),
                       queue_(),
                       locks_(),
                       mmap_limit_() {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
