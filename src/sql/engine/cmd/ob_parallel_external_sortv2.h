/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_
#define OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_

#include "lib/compress/lz4/ob_lz4_compressor.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_encode_decode.hpp"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_define.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include <cstdint>
#include <cstring>

namespace oceanbase {
namespace sql {

struct ObExternalSortConstant {
  static const int64_t BUF_HEADER_LENGTH =
      sizeof(int64_t); // serialization::encoded_length_i64(0);
  static const int64_t MIN_MEMORY_LIMIT = (1LL << 20) * 8LL;
  static const int64_t DEFAULT_FILE_READ_WRITE_BUFFER = 2 * 1024 * 1024LL; // 2m
  static const int64_t MIN_MULTIPLE_MERGE_COUNT = 2;
  // static const int64_t DECOMPRESS_BUFFER_SIZE = 4 * 1024LL * 1024LL;
  static const int64_t ALIGN_SIZE = (1LL * 10) * 4LL;
  static const int64_t MAX_SERIALIZE_SIZE = 500;
  static inline int get_io_timeout_ms(const int64_t expire_timestamp,
                                      int64_t &wait_time_ms);
  static inline bool is_timeout(const int64_t expire_timestamp);
};

int ObExternalSortConstant::get_io_timeout_ms(const int64_t expire_timestamp,
                                              int64_t &wait_time_ms) {
  int ret = common::OB_SUCCESS;

  wait_time_ms =
      std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (expire_timestamp < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(expire_timestamp));
  } else if (0 != expire_timestamp) {
    const int64_t left_time_ms =
        (expire_timestamp - common::ObTimeUtility::current_time()) / 1000;
    if (0 == left_time_ms) {
      wait_time_ms =
          -1; // 0 means use default io timeout limit, so set -1 instead
    } else {
      wait_time_ms = std::min(wait_time_ms, left_time_ms);
    }
  }
  return ret;
}

bool ObExternalSortConstant::is_timeout(const int64_t expire_timestamp) {
  bool is_timeout = false;
  if (0 != expire_timestamp) {
    const int64_t cur_time = common::ObTimeUtility::current_time();
    if (cur_time > expire_timestamp) {
      is_timeout = true;
    }
  }
  return is_timeout;
}

// static int int_encode(const common::ObArray<common::ObDatum *>
// &src_datums,
//                       char *dest_buf, int64_t &pos, const int64_t cap) {
//   int ret = OB_SUCCESS;
//   size_t i = 0;
//   for (; i < src_datums.size() && OB_SUCC(ret); i++) {
//     if (OB_FAIL(src_datums[i]->serialize(dest_buf, cap, pos))) {
//       STORAGE_LOG(WARN, "fail to serialize item", K(ret));
//     }
//   }
//   return ret;
// };

// class IntergerEncode : public iEncode {

// }

// class UIntergerEncode : public iEncode {

// }

template <typename T> class ObFragmentIterator {

public:
  ObFragmentIterator() {}
  virtual ~ObFragmentIterator() {}
  virtual int get_next_item(const T *&item) = 0;
  virtual int prefetch_all() { return common::OB_SUCCESS; }
  virtual int clean_up() { return common::OB_SUCCESS; }
  virtual int prefetch() { return common::OB_SUCCESS; }
  virtual TO_STRING_KV(K(""));
};

template <typename T = oceanbase::blocksstable::ObStorageDatum>
class ObMacroBufferWriter {
public:
  ObMacroBufferWriter();
  virtual ~ObMacroBufferWriter();
  int write_item(const T &item);
  int assign(const int64_t buf_pos, const int64_t buf_cap, char *buf,
             encode_func encoder);
  int serialize_header(bool is_first_write);
  bool has_item();
  int64_t size();
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_cap_));

private:
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_cap_;
  encode_func encoder_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBufferWriter);
};

template <typename T>
ObMacroBufferWriter<T>::ObMacroBufferWriter()
    : buf_(NULL), buf_pos_(0), buf_cap_(0) {}

template <typename T> ObMacroBufferWriter<T>::~ObMacroBufferWriter() {}

template <typename T> int ObMacroBufferWriter<T>::write_item(const T &item) {
  // 保留 32 KB，避免编码后长度比原来长，内存越界的情况
  static const int64_t reserve_size= (1LL<<10)*32;
  int ret = common::OB_SUCCESS;

  const int64_t estimate_size = item.get_serialize_size();
  // 可能存在编码后的长度比原本长的情况，甚至超过 buf 容量
  // 一般假设不会发生
  if (estimate_size + buf_pos_ + reserve_size > buf_cap_) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(encoder_(item, buf_, buf_pos_, buf_cap_))) {
    STORAGE_LOG(WARN, "fail to push back", K(ret));
  }
  return ret;
}

template <typename T>
int ObMacroBufferWriter<T>::serialize_header(bool is_first_write) {
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObExternalSortConstant::BUF_HEADER_LENGTH;
  int64_t tmp_pos_ = 0;
  if (OB_FAIL(common::serialization::encode_i64(buf_, header_size, tmp_pos_,
                                                buf_pos_))) {
    STORAGE_LOG(WARN, "fail to encode macro block buffer header", K(ret),
                K(tmp_pos_), K(header_size), K(buf_pos_));
  } else {

    STORAGE_LOG(DEBUG, "serialize header success", K(tmp_pos_), K(buf_pos_));
  }
  if (likely(!is_first_write)) {
    memcpy(buf_ + 8, buf_, 8);
  }
  return ret;
}

template <typename T>
int ObMacroBufferWriter<T>::assign(const int64_t pos, const int64_t buf_cap,
                                   char *buf, encode_func encoder) {
  buf_pos_ = pos;
  buf_cap_ = buf_cap;
  buf_ = buf;
  encoder_ = encoder;
  return OB_SUCCESS;
}

template <typename T> bool ObMacroBufferWriter<T>::has_item() {
  //  return buf_pos_ > ObExternalSortConstant::BUF_HEADER_LENGTH;
  return buf_pos_ > ObExternalSortConstant::BUF_HEADER_LENGTH;
}

template <typename T> int64_t ObMacroBufferWriter<T>::size() {
  return buf_pos_;
}

template <typename T, typename C = blocksstable::ObStorageDatum>
class ObFragmentWriterV2 {
public:
  ObFragmentWriterV2();
  virtual ~ObFragmentWriterV2();
  int open(const int64_t buf_size, const int64_t expire_timestamp,
           const uint64_t tenant_id, const int64_t dir_id, const int count,
           const ObArray<ObColDesc> &col_descs);
  int write_item(const T &item);
  int sync();
  void reset();
  int count() const { return count_; }
  int64_t get_fd(int idx) const { return fds_[idx]; }
  const int64_t *get_fds() const { return fds_; }
  int64_t get_dir_id() const { return dir_id_; }
  const T &get_sample_item() const { return sample_item_; }
  const int64_t *get_first_buf_size() const { return first_buf_size_; }
  int64_t get_max_buf_size() const { return max_buf_size_; }

private:
  int flush_buffer(int idx);
  int check_need_flush(bool &need_flush, int idx);

private:
  static const int MAX_COL_LEN = 16;
  bool is_inited_;
  int count_;
  common::ObArray<ObColDesc> col_descs_;
  common::ObArray<char *> bufs_;
  common::ObArray<char *> compress_bufs_;
  common::ObArray<encode_func> encode_funcs_;
  int64_t buf_size_;
  int64_t max_buf_size_;
  int64_t compress_buf_size_;
  ObMacroBufferWriter<C> *macro_buffer_writers_;
  blocksstable::ObTmpFileIOHandle *file_io_handles_;
  int64_t *fds_;
  int64_t expire_timestamp_;
  common::ObArenaAllocator allocator_;
  bool has_sample_item_;
  T sample_item_;
  int64_t dir_id_;
  uint64_t tenant_id_;
  int64_t first_buf_size_[MAX_COL_LEN];
  bool is_first_write_[MAX_COL_LEN];
  common::ObLZ4Compressor191 compressor_;
  DISALLOW_COPY_AND_ASSIGN(ObFragmentWriterV2);
};

template <typename T, typename C>
ObFragmentWriterV2<T, C>::ObFragmentWriterV2()
    : is_inited_(false), bufs_(), compress_bufs_(), buf_size_(0),
      max_buf_size_(0), compress_buf_size_(0), macro_buffer_writers_(NULL),
      file_io_handles_(NULL), fds_(NULL), expire_timestamp_(0),
      allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      has_sample_item_(false), sample_item_(), dir_id_(-1),
      tenant_id_(common::OB_INVALID_ID), compressor_(){};

template <typename T, typename C>
ObFragmentWriterV2<T, C>::~ObFragmentWriterV2() {
  reset();
}

template <typename T, typename C>
int ObFragmentWriterV2<T, C>::open(const int64_t buf_size,
                                   const int64_t expire_timestamp,
                                   const uint64_t tenant_id,
                                   const int64_t dir_id, const int count,
                                   const ObArray<ObColDesc> &col_descs) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObFragmentWriter has already been inited", K(ret));
  } else if (buf_size < ObExternalSortConstant::ALIGN_SIZE ||
             buf_size % DIO_ALIGN_SIZE != 0 || expire_timestamp < 0 ||
             common::OB_INVALID_ID == tenant_id) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf_size),
                K(expire_timestamp));
  } else {
    dir_id_ = dir_id;
    count_ = count;
    col_descs_ = col_descs;
    char *buf_ = NULL;
    char *compress_buf_ = NULL;
    const int64_t align_buf_size =
        common::lower_align(buf_size, ObExternalSortConstant::ALIGN_SIZE);
    const int64_t compress_align_buf_size =
        align_buf_size + align_buf_size / 255 + 32;
    memset(is_first_write_, 1, sizeof(bool) * MAX_COL_LEN);

    if (OB_ISNULL((buf_ = static_cast<char *>(
                       allocator_.alloc(align_buf_size * count_))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate buffer", K(ret), K(align_buf_size));
    } else if (OB_ISNULL((compress_buf_ = static_cast<char *>(allocator_.alloc(
                              compress_align_buf_size * count_))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate buffer", K(ret), K(align_buf_size));
    } else if (OB_ISNULL(
                   macro_buffer_writers_ =
                       static_cast<ObMacroBufferWriter<C> *>(allocator_.alloc(
                           sizeof(ObMacroBufferWriter<C>) * count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_ISNULL(file_io_handles_ =
                             static_cast<blocksstable::ObTmpFileIOHandle *>(
                                 allocator_.alloc(
                                     sizeof(blocksstable::ObTmpFileIOHandle) *
                                     count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_ISNULL(fds_ = static_cast<int64_t *>(
                             allocator_.alloc(sizeof(int64_t) * count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else {
      buf_size_ = align_buf_size;
      compress_buf_size_ = compress_align_buf_size;
      new (macro_buffer_writers_) ObMacroBufferWriter<T>[ count_ ];
      new (file_io_handles_) blocksstable::ObTmpFileIOHandle[count_];
      for (int i = 0; i < count_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(bufs_.push_back(buf_ + i * align_buf_size))) {
          STORAGE_LOG(WARN, "fail to push back", K(ret));
        } else if (OB_FAIL(compress_bufs_.push_back(
                       compress_buf_ + i * compress_align_buf_size))) {
          STORAGE_LOG(WARN, "fail to push back", K(ret));
        } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(fds_[i], dir_id_))) {
          STORAGE_LOG(WARN, "fail to open file", K(ret));
        } else {
          encode_func encoder = NULL;
          switch (col_descs_[i].col_type_.get_type()) {
          case ObObjType::ObInt32Type:
          case ObObjType::ObIntType:
          //   encoder = int_encode;
          //   break;
          // // DECIMAL
          case ObObjType::ObNumberType:
          case ObObjType::ObDateType:
          case ObObjType::ObVarcharType:
          case ObObjType::ObCharType:
          default:
            encoder = default_encode;
            break;
          }
          encode_funcs_.push_back(encoder);
          macro_buffer_writers_[i].assign(
              ObExternalSortConstant::BUF_HEADER_LENGTH, buf_size_, bufs_[i],
              encoder);
        }
      }
      expire_timestamp_ = expire_timestamp;
      has_sample_item_ = false;
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T, typename C>
int ObFragmentWriterV2<T, C>::write_item(const T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentWriter has not been inited", K(ret));
  }
  for (int i = 0; i < count_ && OB_SUCC(ret); i++) {
    if (OB_FAIL(macro_buffer_writers_[i].write_item(item.datums_[i]))) {
      if (OB_UNLIKELY(ret != OB_EAGAIN)) {
        STORAGE_LOG(WARN, "fail to write item", K(ret));
      } else {
        if (OB_FAIL(flush_buffer(i))) {
          STORAGE_LOG(WARN, "switch next macro buffer failed", K(ret));
        } else if (OB_FAIL(
                       macro_buffer_writers_[i].write_item(item.datums_[i]))) {
          STORAGE_LOG(WARN, "fail to write item", K(ret));
        }
      }
    }
  }
  STORAGE_LOG(INFO, "write_item", K(item));

  if (OB_SUCC(ret) && !has_sample_item_) {
    const int64_t buf_len =
        item.get_deep_copy_size(); // deep copy size may be 0
    char *buf = NULL;
    int64_t pos = 0;
    if (buf_len > 0 &&
        OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(sample_item_.deep_copy(item, buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy item", K(ret));
    } else {
      has_sample_item_ = true;
    }
  }
  return ret;
}

template <typename T, typename C>
int ObFragmentWriterV2<T, C>::check_need_flush(bool &need_flush, int idx) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentWriterV2 has not been inited", K(ret));
  } else {
    need_flush = macro_buffer_writers_[idx].has_item();
  }
  return ret;
}

template <typename T, typename C>
int ObFragmentWriterV2<T, C>::flush_buffer(int idx) {
  int ret = common::OB_SUCCESS;
  int64_t timeout_ms = 0;
  int64_t compress_size = 0;
  int64_t data_offset = is_first_write_[idx]
                            ? ObExternalSortConstant::BUF_HEADER_LENGTH
                            : 2 * ObExternalSortConstant::BUF_HEADER_LENGTH;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentWriterV2 has not been inited", K(ret));
  } else if (OB_FAIL(ObExternalSortConstant::get_io_timeout_ms(
                 expire_timestamp_, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret),
                K(expire_timestamp_));
  } else if (OB_FAIL(file_io_handles_[idx].wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
  } else if (OB_FAIL(compressor_.compress(
                 bufs_[idx] + ObExternalSortConstant::BUF_HEADER_LENGTH,
                 macro_buffer_writers_[idx].size() -
                     ObExternalSortConstant::BUF_HEADER_LENGTH,
                 compress_bufs_[idx] + data_offset, compress_buf_size_,
                 compress_size))) {
    STORAGE_LOG(WARN, "fail to compress", K(compress_size), K(buf_size_));
  } else {
    STORAGE_LOG(INFO,"[OB_COMPRESS_INFO]" ,"idx",idx,"compress_size", compress_size/1024, "origin_size",
                macro_buffer_writers_[idx].size()/1024);
    int64_t tmp_pos_ = 0;
    if (OB_FAIL(common::serialization::encode_i64(
            compress_bufs_[idx], ObExternalSortConstant::BUF_HEADER_LENGTH,
            tmp_pos_,
            compress_size + ObExternalSortConstant::BUF_HEADER_LENGTH))) {
      STORAGE_LOG(WARN, "fail to encode buffer header", K(ret));
    } else {
      blocksstable::ObTmpFileIOInfo io_info;
      io_info.fd_ = fds_[idx];
      io_info.dir_id_ = dir_id_;
      if (OB_LIKELY(!is_first_write_[idx])) {
        memcpy(compress_bufs_[idx] + ObExternalSortConstant::BUF_HEADER_LENGTH,
               compress_bufs_[idx], ObExternalSortConstant::BUF_HEADER_LENGTH);
      } else {
        first_buf_size_[idx] = compress_size + data_offset;
        is_first_write_[idx] = false;
      }
      max_buf_size_ = std::max(compress_size + data_offset, max_buf_size_);
      io_info.size_ = compress_size + data_offset;
      io_info.tenant_id_ = tenant_id_;
      io_info.buf_ = compress_bufs_[idx];
      io_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
      io_info.io_desc_.set_wait_event(
          ObWaitEventIds::DB_FILE_INDEX_BUILD_WRITE);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_write(io_info,
                                                     file_io_handles_[idx]))) {
        STORAGE_LOG(WARN, "fail to do aio write macro file", K(ret),
                    K(io_info));
      } else {
        macro_buffer_writers_[idx].assign(
            ObExternalSortConstant::BUF_HEADER_LENGTH, buf_size_, bufs_[idx],
            encode_funcs_[idx]);
      }
    }
  }
  return ret;
}

template <typename T, typename C> int ObFragmentWriterV2<T, C>::sync() {
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    for (int i = 0; i < count_; i++) {
      bool need_flush = false;
      if (OB_FAIL(check_need_flush(need_flush, i))) {
        STORAGE_LOG(WARN, "fail to check need flush", K(ret));
      } else if (need_flush) {
        if (OB_FAIL(flush_buffer(i))) {
          STORAGE_LOG(WARN, "fail to flush buffer", K(ret));
        }
      }
      int64_t data_end = 0;
      blocksstable::ObTmpFileIOInfo io_info;
      io_info.fd_ = fds_[i];
      io_info.dir_id_ = dir_id_;
      io_info.size_ = 8;
      io_info.tenant_id_ = tenant_id_;
      io_info.buf_ = (char *)&data_end;
      io_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
      io_info.io_desc_.set_wait_event(
          ObWaitEventIds::DB_FILE_INDEX_BUILD_WRITE);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_write(io_info,
                                                     file_io_handles_[i]))) {
        STORAGE_LOG(WARN, "fail to do aio write macro file", K(ret),
                    K(io_info));
      }

      if (OB_SUCC(ret)) {
        int64_t timeout_ms = 0;
        if (OB_FAIL(ObExternalSortConstant::get_io_timeout_ms(expire_timestamp_,
                                                              timeout_ms))) {
          STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret),
                      K(expire_timestamp_));
        } else if (OB_FAIL(file_io_handles_[i].wait(timeout_ms))) {
          STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
        } else if (OB_FAIL(ObExternalSortConstant::get_io_timeout_ms(
                       expire_timestamp_, timeout_ms))) {
          STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret),
                      K(expire_timestamp_));
        } else if (OB_FAIL(
                       FILE_MANAGER_INSTANCE_V2.sync(fds_[i], timeout_ms))) {
          STORAGE_LOG(WARN, "fail to sync macro file", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename T, typename C> void ObFragmentWriterV2<T, C>::reset() {
  is_inited_ = false;
  count_ = 0;
  col_descs_.reset();
  bufs_.reset();
  compress_bufs_.reset();
  encode_funcs_.reset();
  buf_size_ = 0;
  max_buf_size_ = 0;
  compress_buf_size_ = 0;
  macro_buffer_writers_ = NULL;
  file_io_handles_ = NULL;
  fds_ = NULL;

  expire_timestamp_ = 0;
  allocator_.reuse();
  has_sample_item_ = false;
  memset(first_buf_size_, 0, sizeof(int64_t) * MAX_COL_LEN);
  memset(is_first_write_, 1, sizeof(bool) * MAX_COL_LEN);
  dir_id_ = -1;
  tenant_id_ = common::OB_INVALID_ID;
}

template <typename T> class ObMacroBufferReader {
public:
  ObMacroBufferReader();
  virtual ~ObMacroBufferReader();
  int read_item(T &item, int idx);
  int deserialize_header();
  int deserialize_next_header(char *buf, int64_t &next_buf_len);
  void assign(const int64_t buf_pos, const int64_t buf_cap, const char *buf);
  void assignv2(const int64_t buf_pos, const int64_t buf_cap);
  int64_t get_next_buf_size();
  void set_next_buf_size(int64_t len) { next_buf_len_ = len; }
  int init_decompress_buffer(common::ObArenaAllocator &allocator, const int64_t buf_size);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_len_), K(buf_cap_));

private:
  common::ObLZ4Compressor compressor_;
  const char *buf_;
  char *decompress_buf_;
  int64_t decompress_buf_len_;
  int64_t buf_pos_;
  int64_t buf_len_;
  int64_t buf_cap_;
  int64_t next_buf_len_;
};

template <typename T>
int ObMacroBufferReader<T>::init_decompress_buffer(common::ObArenaAllocator &allocator,
                                          const int64_t buf_size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decompress_buf_)) {
    if (OB_ISNULL(decompress_buf_ =
                      static_cast<char *>(allocator.alloc(buf_size)))) {
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret), K(buf_size));
    } else {
      decompress_buf_len_ = buf_size;
    }
  }
  return ret;
}

template <typename T> int64_t ObMacroBufferReader<T>::get_next_buf_size() {
  buf_len_ = next_buf_len_;
  buf_pos_ = 8;
  if (buf_len_ < 0) {
    STORAGE_LOG(WARN, "deserialize header failed");
  }
  int64_t decompress_size = 0;
  int ret = OB_SUCCESS;
  next_buf_len_ = 0;
  if (OB_FAIL(
          compressor_.decompress(buf_ + 8, buf_len_ - 8, decompress_buf_ + 8,
                                 decompress_buf_len_ - 8, decompress_size))) {
    STORAGE_LOG(WARN, "fail to decompress", KR(ret), K(buf_len_),
                K(decompress_buf_len_));
  } else {
    char *next_buf_start = const_cast<char *>(buf_) + buf_len_;
    memcpy(decompress_buf_, buf_, 8);
    buf_ = decompress_buf_;
    buf_len_ = decompress_size + 8;
    deserialize_next_header(next_buf_start, next_buf_len_);
  }
  return next_buf_len_;
}

template <typename T>
ObMacroBufferReader<T>::ObMacroBufferReader()
    : buf_(NULL), decompress_buf_(NULL), decompress_buf_len_(0), buf_pos_(0),
      buf_len_(0), buf_cap_(0) {
  // decompress_buf_ = new char[2LL<<20];
}

template <typename T> ObMacroBufferReader<T>::~ObMacroBufferReader() {
  // if (decompress_buf_ != nullptr) {
  //  delete decompress_buf_;
  //}
}

template <typename T> int ObMacroBufferReader<T>::read_item(T &item, int idx) {
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (buf_pos_ == buf_len_) {
      ret = common::OB_EAGAIN;
    } else if (OB_FAIL(
                   item.datums_[idx].deserialize(buf_, buf_len_, buf_pos_))) {
      STORAGE_LOG(WARN, "fail to deserialize buffer", K(ret), K(buf_len_),
                  K(buf_pos_));
    } else {
      STORAGE_LOG(DEBUG, "macro buffer reader", K(buf_len_), K(buf_pos_));
    }
  }
  return ret;
}

template <typename T> int ObMacroBufferReader<T>::deserialize_header() {
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObExternalSortConstant::BUF_HEADER_LENGTH;
  if (OB_FAIL(common::serialization::decode_i64(buf_, header_size, buf_pos_,
                                                &buf_len_))) {
    STORAGE_LOG(WARN, "fail to encode macro block buffer header", K(ret),
                K(buf_pos_), K(header_size), K(buf_len_));
  } else {
    STORAGE_LOG(DEBUG, "deserialize header success", K(buf_len_), K(buf_pos_));
  }
  return ret;
}

template <typename T>
int ObMacroBufferReader<T>::deserialize_next_header(char *buf,
                                                    int64_t &next_buf_len) {
  int ret = common::OB_SUCCESS;
  const int64_t header_size = ObExternalSortConstant::BUF_HEADER_LENGTH;
  int64_t tmp_pos = 0;
  if (OB_FAIL(common::serialization::decode_i64(buf, header_size, tmp_pos,
                                                &next_buf_len))) {
    STORAGE_LOG(WARN, "fail to encode macro block buffer header", K(ret),
                K(header_size), K(next_buf_len));
  } else {
    STORAGE_LOG(DEBUG, "deserialize header success", K(buf_len_), K(buf_pos_));
  }
  return ret;
}

template <typename T>
void ObMacroBufferReader<T>::assign(const int64_t buf_pos,
                                    const int64_t buf_cap, const char *buf) {
  buf_pos_ = buf_pos;
  buf_cap_ = buf_cap;
  buf_len_ = 0;
  buf_ = buf;
}

template <typename T>
void ObMacroBufferReader<T>::assignv2(const int64_t buf_pos,
                                      const int64_t buf_cap) {
  buf_pos_ = buf_pos;
  buf_cap_ = buf_cap;
  buf_len_ = 0;
}

template <typename T> class ObFragmentReaderV2 : public ObFragmentIterator<T> {
public:
  ObFragmentReaderV2();
  virtual ~ObFragmentReaderV2();
  int init(const int64_t *fds, const int64_t dir_id,
           const int64_t expire_timestamp, const uint64_t tenant_id,
           const T &sample_item, const int64_t buf_size,
           const int64_t *first_buf_sizes, const int count,
           const ObArray<ObColDesc> &col_descs);
  int open();
  virtual int get_next_item(const T *&item);
  virtual int clean_up();

private:
  int prefetch(int idx);
  int prefetch_all();
  int wait_all();
  int wait(int idx);
  int pipeline_all();
  int pipeline(int idx);
  void reset();

private:
  static const int64_t MAX_HANDLE_COUNT = 2;
  static const int MAX_COL_LEN = 16;
  bool is_inited_;
  int count_;
  ObArray<ObColDesc> col_descs_;
  char *bufs_[MAX_COL_LEN];
  ObMacroBufferReader<T> *macro_buffer_readers_;
  blocksstable::ObTmpFileIOHandle file_io_handles_[MAX_COL_LEN]
                                                  [MAX_HANDLE_COUNT];
  int64_t handle_cursors_[MAX_COL_LEN];
  int64_t *fds_;
  int64_t *first_buf_sizes_;

  T curr_item_;

  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator sample_allocator_;
  int64_t dir_id_;
  int64_t expire_timestamp_;
  uint64_t tenant_id_;
  bool is_prefetch_end_[MAX_COL_LEN];
  int64_t buf_size_;
  bool is_first_prefetch_;
  bool is_open_prefetch_[MAX_COL_LEN];

public:
  int64_t get_next_buf_size(int idx) {
    return macro_buffer_readers_[idx].get_next_buf_size();
  }
};

template <typename T>
ObFragmentReaderV2<T>::ObFragmentReaderV2()
    : is_inited_(false), count_(0), col_descs_(), macro_buffer_readers_(NULL),
      fds_(NULL), first_buf_sizes_(NULL), curr_item_(),
      allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      sample_allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                        OB_MALLOC_NORMAL_BLOCK_SIZE),
      dir_id_(-1), expire_timestamp_(0), tenant_id_(common::OB_INVALID_ID),
      buf_size_(0), is_first_prefetch_(true) {
  memset(bufs_, 0, sizeof(void *) * MAX_COL_LEN);
  memset(is_prefetch_end_, 0, sizeof(bool) * MAX_COL_LEN);
  memset(is_open_prefetch_, 1, sizeof(bool) * MAX_COL_LEN);
};

template <typename T> ObFragmentReaderV2<T>::~ObFragmentReaderV2() { reset(); }

template <typename T>
int ObFragmentReaderV2<T>::init(const int64_t *fds, const int64_t dir_id,
                                const int64_t expire_timestamp,
                                const uint64_t tenant_id, const T &sample_item,
                                const int64_t buf_size,
                                const int64_t *first_buf_sizes, const int count,
                                const ObArray<ObColDesc> &col_descs) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObFragmentReader has already been inited", K(ret));
  } else if (common::OB_INVALID_ID == tenant_id ||
             buf_size % DIO_ALIGN_SIZE != 0 || expire_timestamp < 0 ||
             buf_size < 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id),
                K(expire_timestamp), K(buf_size));
  } else {
    dir_id_ = dir_id;
    count_ = count;
    col_descs_ = col_descs;
    const int64_t buf_len =
        sample_item.get_deep_copy_size(); // deep copy size may be 0
    int64_t pos = 0;
    char *buf = NULL;
    if (buf_len > 0 && OB_ISNULL(buf = static_cast<char *>(
                                     sample_allocator_.alloc(buf_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(curr_item_.deep_copy(sample_item, buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy item", K(ret));
    } else if (OB_ISNULL(
                   macro_buffer_readers_ =
                       static_cast<ObMacroBufferReader<T> *>(allocator_.alloc(
                           sizeof(ObMacroBufferReader<T>) * count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_ISNULL(fds_ = static_cast<int64_t *>(
                             allocator_.alloc(sizeof(int64_t) * count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_ISNULL(first_buf_sizes_ = static_cast<int64_t *>(
                             allocator_.alloc(sizeof(int64_t) * count_)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else {
      memset(handle_cursors_, 0, sizeof(int64_t) * MAX_COL_LEN);
      memcpy(fds_, fds, count_ * sizeof(int64_t));
      memcpy(first_buf_sizes_, first_buf_sizes, count_ * sizeof(int64_t));
      new (macro_buffer_readers_) ObMacroBufferReader<T>[ count_ ];
      tenant_id_ = tenant_id;
      is_first_prefetch_ = true;
      buf_size_ =
          common::lower_align(buf_size, ObExternalSortConstant::ALIGN_SIZE);
      for (int i = 0; i < count_; i++) {
        macro_buffer_readers_[i].set_next_buf_size(first_buf_sizes[i]);
      }
      expire_timestamp_ = expire_timestamp;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::prefetch_all() {
  int ret = OB_SUCCESS;
  for (int i = 0; i < count_; i++) {
    if (OB_FAIL(prefetch(i))) {
      STORAGE_LOG(WARN, "fail tp prefetch data", K(ret), K(i));
    }
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::open() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else if (OB_FAIL(prefetch_all())) {
    STORAGE_LOG(WARN, "fail to prefetch data", K(ret));
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::prefetch(int idx) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else {
    if (nullptr == bufs_[idx]) {
      if (OB_ISNULL(bufs_[idx] =
                        static_cast<char *>(allocator_.alloc(buf_size_/2)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
      }
    }
    if (OB_UNLIKELY(is_first_prefetch_)) {
      if (OB_FAIL(macro_buffer_readers_[idx].init_decompress_buffer(
              allocator_, buf_size_+32))) {
        STORAGE_LOG(WARN, "fail to init decompress buffer", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      blocksstable::ObTmpFileIOInfo io_info;
      io_info.fd_ = fds_[idx];
      io_info.dir_id_ = dir_id_;
      if (unlikely(is_open_prefetch_[idx])) {
        io_info.size_ = first_buf_sizes_[idx] + 8;
        is_open_prefetch_[idx] = false;
      } else {
        io_info.size_ = get_next_buf_size(idx) + 8;
      }
      // STORAGE_LOG(INFO, "prefetch size", K(io_info.size_));
      io_info.tenant_id_ = tenant_id_;
      io_info.buf_ = bufs_[idx];
      io_info.io_desc_.set_category(common::ObIOCategory::SYS_IO);
      io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_INDEX_BUILD_READ);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.aio_read(
              io_info, file_io_handles_[idx][handle_cursors_[idx] %
                                             MAX_HANDLE_COUNT]))) {
        if (common::OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to do aio read from macro file", K(ret),
                      K(fds_[idx]));
        } else {
          is_prefetch_end_[idx] = true;
          ret = OB_SUCCESS;
        }
      } else {
        ++handle_cursors_[idx];
      }
    }
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::wait(int idx) {
  int ret = common::OB_SUCCESS;
  const int64_t wait_cursor = (handle_cursors_[idx] + 1) % MAX_HANDLE_COUNT;
  int64_t timeout_ms = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else if (is_prefetch_end_[idx]) {
    ret = common::OB_ITER_END;
  } else if (OB_FAIL(ObExternalSortConstant::get_io_timeout_ms(
                 expire_timestamp_, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret), K(expire_timestamp_),
                K(timeout_ms));
  } else if (OB_FAIL(file_io_handles_[idx][wait_cursor].wait(timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
  } else {
    macro_buffer_readers_[idx].assign(
        0, buf_size_/2, file_io_handles_[idx][wait_cursor].get_buffer());
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::wait_all() {
  int ret = common::OB_SUCCESS;
  for (int i = 0; i < count_; i++) {
    const int64_t wait_cursor = (handle_cursors_[i] + 1) % MAX_HANDLE_COUNT;
    int64_t timeout_ms = 0;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
    } else if (is_prefetch_end_[i]) {
      ret = common::OB_ITER_END;
    } else if (OB_FAIL(ObExternalSortConstant::get_io_timeout_ms(
                   expire_timestamp_, timeout_ms))) {
      STORAGE_LOG(WARN, "fail to get io timeout ms", K(ret),
                  K(expire_timestamp_), K(timeout_ms));
    } else if (OB_FAIL(file_io_handles_[i][wait_cursor].wait(timeout_ms))) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(timeout_ms));
    } else {
      macro_buffer_readers_[i].assign(
          0, buf_size_/2, file_io_handles_[i][wait_cursor].get_buffer());
    }
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::pipeline_all() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else if (OB_FAIL(wait_all())) { // switch next buffer
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
    }
  } else if (OB_FAIL(prefetch_all())) { // perfetch next next
                                        // buffer,里面解析next buffer的header
    STORAGE_LOG(WARN, "fail to prefetch data", K(ret));
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::pipeline(int idx) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else if (OB_FAIL(wait(idx))) { // switch next buffer
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to wait io finish", K(ret));
    }
  } else if (OB_FAIL(prefetch(idx))) { // perfetch next next
                                       // buffer,里面解析next buffer的header
    STORAGE_LOG(WARN, "fail to prefetch data", K(ret));
  }
  return ret;
}

template <typename T> int ObFragmentReaderV2<T>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentReaderV2 has not been inited", K(ret));
  } else if (is_first_prefetch_) {
    if (OB_FAIL(pipeline_all())) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to pipeline data", K(ret));
      }
    } else {
      is_first_prefetch_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    for (int idx = 0; idx < count_; idx++) {
      if (OB_FAIL(macro_buffer_readers_[idx].read_item(curr_item_, idx))) {
        if (common::OB_EAGAIN == ret) {
          if (OB_FAIL(pipeline(idx))) {
            if (common::OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "fail to switch next buffer", K(ret));
            }
          } else if (OB_FAIL(macro_buffer_readers_[idx].read_item(curr_item_,
                                                                  idx))) {
            STORAGE_LOG(WARN, "fail to read item", K(ret));
          }
        }
      }
    }
    curr_item_.count_ = count_;
  }

  if (OB_SUCC(ret)) {
    item = &curr_item_;
  }
  return ret;
}

template <typename T> void ObFragmentReaderV2<T>::reset() {
  is_inited_ = false;
  // memset(is_decompress_buffer_inited_, 0, sizeof(bool) * MAX_COL_LEN);
  expire_timestamp_ = 0;
  allocator_.reset();
  sample_allocator_.reset();
  // for (int i = 0; i < count_; i++) {
  //  macro_buffer_readers_[i].assign(0, 0, NULL);
  //}
  macro_buffer_readers_ = NULL;
  // macro_buffer_reader_.assignv2(0,0);
  // fd_ = -1;
  fds_ = NULL;
  dir_id_ = -1;
  // file_io_handles_ = NULL;
  for (int64_t i = 0; i < MAX_COL_LEN; i++) {
    for (int64_t j = 0; j < MAX_HANDLE_COUNT; ++j) {
      file_io_handles_[i][j].reset();
    }
  }

  memset(handle_cursors_, 0, sizeof(int64_t) * MAX_COL_LEN);
  // memset(first_buf_sizes_, 0, sizeof(int64_t) * MAX_COL_LEN);
  // memset(max_buf_sizes_, 0, sizeof(int64_t) * MAX_COL_LEN);
  // bufs_.reset();
  memset(bufs_, 0, sizeof(void *) * MAX_COL_LEN);
  tenant_id_ = common::OB_INVALID_ID;
  // is_prefetch_end_ = false;
  memset(is_prefetch_end_, 0, sizeof(bool) * MAX_COL_LEN);
  memset(is_open_prefetch_, 1, sizeof(bool) * MAX_COL_LEN);
  buf_size_ = 0;
  is_first_prefetch_ = true;
}

template <typename T> int ObFragmentReaderV2<T>::clean_up() {
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    for (int i = 0; i < count_; i++) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(fds_[i]))) {
        STORAGE_LOG(WARN, "fail to remove macro file", K(ret));
      }
    }
    reset();
  }
  return ret;
}

template <typename T, typename Compare> class ObFragmentMerge {
public:
  typedef ObFragmentIterator<T> FragmentIterator;
  static const int64_t DEFAULT_ITERATOR_NUM = 64;
  ObFragmentMerge();
  virtual ~ObFragmentMerge();
  int init(const common::ObIArray<FragmentIterator *> &readers,
           Compare *compare);
  int open();
  int get_next_item(const T *&item);
  void reset();
  bool is_opened() const { return is_opened_; }

private:
  int direct_get_next_item(const T *&item);
  int heap_get_next_item(const T *&item);
  int build_heap();

private:
  struct HeapItem {
    const T *item_;
    int64_t idx_;
    HeapItem() : item_(NULL), idx_(0) {}
    void reset() {
      item_ = NULL;
      idx_ = 0;
    }
    TO_STRING_KV(K_(item), K_(idx));
  };
  class HeapCompare {
  public:
    explicit HeapCompare(int &ret);
    virtual ~HeapCompare();
    bool operator()(const HeapItem &left_item,
                    const HeapItem &right_item) const;
    void set_compare(Compare *compare) { compare_ = compare; }
    int get_error_code() { return ret_; }

  private:
    Compare *compare_;
    int &ret_;
  };

private:
  bool is_inited_;
  bool is_opened_;
  HeapCompare compare_;
  common::ObSEArray<FragmentIterator *, DEFAULT_ITERATOR_NUM> iters_;
  int64_t last_iter_idx_;
  common::ObBinaryHeap<HeapItem, HeapCompare, DEFAULT_ITERATOR_NUM> heap_;
  int sort_ret_;
};

template <typename T, typename Compare>
ObFragmentMerge<T, Compare>::HeapCompare::HeapCompare(int &ret)
    : compare_(NULL), ret_(ret) {}

template <typename T, typename Compare>
ObFragmentMerge<T, Compare>::HeapCompare::~HeapCompare() {}

template <typename T, typename Compare>
void ObFragmentMerge<T, Compare>::reset() {
  is_inited_ = false;
  is_opened_ = false;
  iters_.reset();
  last_iter_idx_ = -1;
  heap_.reset();
  sort_ret_ = common::OB_SUCCESS;
}

template <typename T, typename Compare>
bool ObFragmentMerge<T, Compare>::HeapCompare::operator()(
    const HeapItem &left_item, const HeapItem &right_item) const {
  int ret = common::OB_SUCCESS;
  bool bret = false;
  if (NULL == compare_) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(compare_));
  } else if (NULL == left_item.item_ || NULL == right_item.item_) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid compare items", K(ret), KP(left_item.item_),
                KP(right_item.item_));
  } else {
    bret = !compare_->operator()(left_item.item_, right_item.item_);
  }
  if (OB_FAIL(ret)) {
    ret_ = ret;
  } else if (OB_FAIL(compare_->result_code_)) {
    ret_ = compare_->result_code_;
  } else {
    ret_ = common::OB_SUCCESS;
  }
  return bret;
}

template <typename T, typename Compare>
ObFragmentMerge<T, Compare>::ObFragmentMerge()
    : is_inited_(false), is_opened_(false), compare_(sort_ret_), iters_(),
      last_iter_idx_(-1), heap_(compare_), sort_ret_(common::OB_SUCCESS) {}

template <typename T, typename Compare>
ObFragmentMerge<T, Compare>::~ObFragmentMerge() {}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::init(
    const common::ObIArray<FragmentIterator *> &iters, Compare *compare) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObFragmentMerge has been inited", K(ret));
  } else if (0 == iters.count() || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(iters.count()),
                KP(compare));
  } else if (OB_FAIL(iters_.assign(iters))) {
    STORAGE_LOG(WARN, "fail to assign iterators", K(ret));
  } else {
    compare_.set_compare(compare);
    is_inited_ = true;
    is_opened_ = false;
  }
  return ret;
}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::open() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentMerge has not been inited", K(ret));
  } else if (is_opened_) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "ObFragmentMerge has been opened before", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      if (OB_FAIL(iters_.at(i)->prefetch_all())) {
        STORAGE_LOG(WARN, "fail to prefetch", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (iters_.count() > 1 && OB_FAIL(build_heap())) {
        STORAGE_LOG(WARN, "fail to build heap", K(ret));
      } else {
        is_opened_ = true;
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::build_heap() {
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  HeapItem heap_item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentMerge has not been inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    if (OB_FAIL(iters_.at(i)->get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret), K(i));
      } else {
        ret = common::OB_SUCCESS;
      }
    } else if (NULL == item) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", K(ret), KP(item));
    } else {
      heap_item.item_ = item;
      heap_item.idx_ = i;
      if (OB_FAIL(heap_.push(heap_item))) {
        STORAGE_LOG(WARN, "fail to push heap", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "push a heap item", K(*item));
      }
    }
  }

  return ret;
}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::direct_get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentMerge has not been inited", K(ret));
  } else if (1 != iters_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(iters_.count()));
  } else if (OB_FAIL(iters_.at(0)->get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::heap_get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  HeapItem heap_item;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentMerge has not been inited", K(ret));
  } else if (last_iter_idx_ >= 0 && last_iter_idx_ < iters_.count()) {
    FragmentIterator *iter = iters_.at(last_iter_idx_);
    if (OB_FAIL(iter->get_next_item(heap_item.item_))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      } else if (OB_FAIL(heap_.pop())) { // overwrite OB_ITER_END
        STORAGE_LOG(WARN, "fail to pop heap item", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        iter->clean_up();
        STORAGE_LOG(DEBUG, "pop a heap item");
      }
    } else if (NULL == heap_item.item_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid item", K(ret), KP(heap_item.item_));
    } else {
      heap_item.idx_ = last_iter_idx_;
      if (OB_FAIL(heap_.replace_top(heap_item))) {
        STORAGE_LOG(WARN, "fail to replace heap top", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        STORAGE_LOG(WARN, "fail to compare items", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "replace heap item", K(*heap_item.item_),
                    K(last_iter_idx_));
      }
    }
    last_iter_idx_ = -1;
  }

  if (OB_SUCC(ret) && heap_.empty()) {
    ret = common::OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    const HeapItem *item_ptr = NULL;
    if (OB_FAIL(heap_.top(item_ptr))) {
      STORAGE_LOG(WARN, "fail to get heap top item", K(ret));
    } else if (OB_FAIL(compare_.get_error_code())) {
      STORAGE_LOG(WARN, "fail to compare items", K(ret));
    } else if (NULL == item_ptr || NULL == item_ptr->item_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid heap item", K(ret), KP(item_ptr));
    } else {
      last_iter_idx_ = item_ptr->idx_;
      item = item_ptr->item_;
      STORAGE_LOG(DEBUG, "top heap item", K(*item), K(last_iter_idx_));
    }
  }

  return ret;
}

template <typename T, typename Compare>
int ObFragmentMerge<T, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObFragmentMerge has not been inited", K(ret));
  } else if (1 == iters_.count()) {
    if (OB_FAIL(direct_get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to directly get next item from reader", K(ret));
      }
    }
  } else if (OB_FAIL(heap_get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item from heap", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare> class ObExternalSortRound {
public:
  ObExternalSortRound();
  virtual ~ObExternalSortRound();
  int init(const int64_t merge_count, const int64_t file_buf_size,
           const int64_t expire_timestamp, const uint64_t tenant_id,
           Compare *compare, const int count,
           const ObArray<ObColDesc> &col_descs);
  bool is_inited() const { return is_inited_; }
  int add_item(const T &item);
  int build_fragment();
  int do_merge(ObExternalSortRound &next_round);
  int do_one_run(const int64_t start_reader_idx,
                 ObExternalSortRound &next_round);
  int finish_write();
  int clean_up();
  int build_merger();
  int get_next_item(const T *&item);
  int64_t get_fragment_count();
  int add_fragment_iter(ObFragmentIterator<T> *iter);
  int transfer_sorted_fragment_iter(ObExternalSortRound &dest_round);
  int transfer_final_sorted_fragment_iter(ObExternalSortRound &dest_round);

private:
  typedef ObFragmentReaderV2<T> FragmentReader;
  typedef ObFragmentIterator<T> FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef ObFragmentWriterV2<T> FragmentWriter;
  typedef ObFragmentMerge<T, Compare> FragmentMerger;
  bool is_inited_;
  int count_;
  ObArray<ObColDesc> col_descs_;
  int64_t merge_count_;
  int64_t file_buf_size_;
  FragmentIteratorList iters_;
  FragmentWriter writer_;
  int64_t expire_timestamp_;
  Compare *compare_;
  FragmentMerger merger_;
  common::ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  bool is_writer_opened_;
};

template <typename T, typename Compare>
ObExternalSortRound<T, Compare>::ObExternalSortRound()
    : is_inited_(false), merge_count_(0), file_buf_size_(0), iters_(),
      writer_(), expire_timestamp_(0), compare_(NULL), merger_(),
      allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      tenant_id_(common::OB_INVALID_ID), dir_id_(-1), is_writer_opened_(false) {
}

template <typename T, typename Compare>
ObExternalSortRound<T, Compare>::~ObExternalSortRound() {}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::init(const int64_t merge_count,
                                          const int64_t file_buf_size,
                                          const int64_t expire_timestamp,
                                          const uint64_t tenant_id,
                                          Compare *compare, const int count,
                                          const ObArray<ObColDesc> &col_descs) {
  int ret = common::OB_SUCCESS;
  allocator_.set_tenant_id(MTL_ID());
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObExternalSortRound has been inited", K(ret));
  } else if (merge_count < ObExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT ||
             file_buf_size % DIO_ALIGN_SIZE != 0 ||
             common::OB_INVALID_ID == tenant_id || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(merge_count),
                K(file_buf_size), KP(compare));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    STORAGE_LOG(WARN, "fail to alloc dir", K(ret));
  } else {
    is_inited_ = true;
    count_ = count;
    col_descs_ = col_descs;
    merge_count_ = merge_count;
    file_buf_size_ = file_buf_size;
    iters_.reset();
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    tenant_id_ = tenant_id;
    is_writer_opened_ = false;
    merger_.reset();
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::add_item(const T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObExternalSortRound timeout", K(ret),
                K(expire_timestamp_));
  } else if (!is_writer_opened_ &&
             OB_FAIL(writer_.open(file_buf_size_, expire_timestamp_, tenant_id_,
                                  dir_id_, count_, col_descs_))) {
    STORAGE_LOG(WARN, "fail to open writer", K(ret), K_(tenant_id), K_(dir_id));
  } else {
    is_writer_opened_ = true;
    if (OB_FAIL(writer_.write_item(item))) {
      STORAGE_LOG(WARN, "fail to write item", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::build_fragment() {
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  FragmentReader *reader = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(FragmentReader)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(reader = new (buf) FragmentReader())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new FragmentReader", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to sync macro file", K(ret));
  } else {
    STORAGE_LOG(INFO, "build fragment", K(writer_.get_sample_item()));
    if (OB_FAIL(reader->init(
            writer_.get_fds(), writer_.get_dir_id(), expire_timestamp_,
            tenant_id_, writer_.get_sample_item(), file_buf_size_,
            writer_.get_first_buf_size(), count_, col_descs_))) {
      STORAGE_LOG(WARN, "fail to open reader", K(ret), K(file_buf_size_),
                  K(expire_timestamp_));
    } else if (OB_FAIL(iters_.push_back(reader))) {
      STORAGE_LOG(WARN, "fail to push back reader", K(ret));
    } else {
      writer_.reset();
      is_writer_opened_ = false;
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::add_fragment_iter(
    ObFragmentIterator<T> *iter) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(iters_.push_back(iter))) {
    STORAGE_LOG(WARN, "fail to add iterator", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::transfer_sorted_fragment_iter(
    ObExternalSortRound<T, Compare> &dest_round) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else {
    int64_t i = 0;
    while (OB_SUCC(ret) && i < iters_.size()) {
      if (OB_FAIL(dest_round.add_fragment_iter(iters_.at(i)))) {
        STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
      }
      i++;
    }
    if (OB_SUCC(ret)) {
      iters_.reset();
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    ObExternalSortRound<T, Compare> &dest_round) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (1 != iters_.count()) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid reader count", K(ret), K(iters_.count()));
  } else {
    if (OB_FAIL(dest_round.add_fragment_iter(iters_.at(0)))) {
      STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
    } else {
      // iter will be freed in dest_round
      iters_.reset();
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::build_merger() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(merger_.init(iters_, compare_))) {
    STORAGE_LOG(WARN, "fail to init FragmentMerger", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::finish_write() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to finish writer", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::do_merge(ObExternalSortRound &next_round) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else {
    int64_t reader_idx = 0;
    STORAGE_LOG(INFO, "external sort do merge start");
    while (OB_SUCC(ret) && reader_idx < iters_.count()) {
      if (OB_FAIL(do_one_run(reader_idx, next_round))) {
        STORAGE_LOG(WARN, "fail to do one run merge", K(ret));
      } else {
        reader_idx += merge_count_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round.finish_write())) {
        STORAGE_LOG(WARN, "fail to finsh next round", K(ret));
      }
    }
    STORAGE_LOG(INFO, "external sort do merge end");
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::do_one_run(
    const int64_t start_reader_idx, ObExternalSortRound &next_round) {
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t end_reader_idx =
        std::min(start_reader_idx + merge_count_, iters_.count());
    FragmentIteratorList iters;
    for (int64_t i = start_reader_idx; OB_SUCC(ret) && i < end_reader_idx;
         ++i) {
      if (OB_FAIL(iters.push_back(iters_.at(i)))) {
        STORAGE_LOG(WARN, "fail to push back iterator list", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      merger_.reset();
      if (OB_FAIL(merger_.init(iters, compare_))) {
        STORAGE_LOG(WARN, "fail to init ObFragmentMerger", K(ret));
      } else if (OB_FAIL(merger_.open())) {
        STORAGE_LOG(WARN, "fail to open merger", K(ret));
      }
    }

    while (OB_SUCC(ret)) {
      share::dag_yield();
      if (OB_FAIL(merger_.get_next_item(item))) {
        if (common::OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "fail to get next item", K(ret));
        } else {
          ret = common::OB_SUCCESS;
          break;
        }
      } else {
        if (OB_FAIL(next_round.add_item(*item))) {
          STORAGE_LOG(WARN, "fail to add item", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round.build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      }
    }

    for (int64_t i = start_reader_idx; i < end_reader_idx; ++i) {
      if (nullptr != iters_[i]) {
        // will do clean up ignore return
        if (common::OB_SUCCESS != (tmp_ret = iters_[i]->clean_up())) {
          STORAGE_LOG(WARN, "fail to do reader clean up", K(tmp_ret), K(i));
        }
        iters_[i]->~ObFragmentIterator();
        iters_[i] = nullptr;
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  } else if (!merger_.is_opened() && OB_FAIL(merger_.open())) {
    STORAGE_LOG(WARN, "fail to open merger", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(merger_.get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int64_t ObExternalSortRound<T, Compare>::get_fragment_count() {
  return iters_.count();
}

template <typename T, typename Compare>
int ObExternalSortRound<T, Compare>::clean_up() {
  int ret = common::OB_SUCCESS;
  int tmp_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSortRound has not been inited", K(ret));
  }

  for (int64_t i = 0; i < iters_.count(); ++i) {
    if (NULL != iters_[i]) {
      if (common::OB_SUCCESS != (tmp_ret = iters_[i]->clean_up())) {
        STORAGE_LOG(WARN, "fail to do reader clean up", K(tmp_ret), K(i));
        ret = (common::OB_SUCCESS == ret) ? tmp_ret : ret;
      }
      iters_[i]->~ObFragmentIterator();
    }
  }

  if (common::OB_SUCCESS != (tmp_ret = writer_.sync())) {
    STORAGE_LOG(WARN, "fail to do writer finish", K(tmp_ret));
    ret = (common::OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  is_inited_ = false;
  count_ = 0;
  merge_count_ = 0;
  file_buf_size_ = 0;
  iters_.reset();
  expire_timestamp_ = 0;
  compare_ = NULL;
  merger_.reset();
  allocator_.reset();
  return ret;
}

template <typename T> class DispatchQueue {
  typedef common::ObSpinLock Lock;
  typedef lib::ObLockGuard<Lock> LockGuard;
  typedef common::ObVector<T *> DatumRowVector;

public:
  DispatchQueue(int64_t mem_limit, uint64_t push_sleep, uint64_t pop_sleep)
      : is_finished(false), push_used(0), push_size(0), pop_size(0),
        pop_index(0), push_alloc_id(0), pop_alloc_id(0),
        buf_mem_limit(mem_limit), push_sleep_time(push_sleep),
        pop_sleep_time(push_sleep){};
  ~DispatchQueue() { reset(); };

  void init(common::ObArenaAllocator *allocator0,
            common::ObArenaAllocator *allocator1) {
    allocator_[0] = allocator0;
    allocator_[1] = allocator1;
    push_alloc = allocator0;
    pop_alloc = allocator0;
  };

  void finish() { is_finished = true; }

  int push_item(const T *item) {
    int ret = OB_SUCCESS;
    char *buf = NULL;
    T *new_item = NULL;
    // 额外多分配了一个单位的内存，第一个内存记录是否已经初始化
    int64_t buf_pos = sizeof(T);
    const int64_t item_size = buf_pos + item->get_deep_copy_size();
    const int64_t alloc_size = 1 + item_size;
    if (OB_ISNULL(buf = static_cast<char *>(alloc(alloc_size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(alloc_size));
    } else if (OB_ISNULL(new_item = new (buf + 1) T())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to placement new item", K(ret));
    } else if (OB_FAIL(
                   new_item->deep_copy(*item, buf + 1, item_size, buf_pos))) {
      STORAGE_LOG(WARN, "fail to deep copy item", K(ret));
    } else {
      buf[0] = 1;
    }

    return ret;
  }

  int pop_item(const T *&item) {
    while (!is_finished && pop_size == push_size)
      usleep(pop_sleep_time);
    if (pop_size < push_size) {
      char *buf = (char *)dispatch_data_[pop_alloc_id].at(pop_index);
      while (buf[0] == 0)
        usleep(pop_sleep_time);
      item = (T *)(buf + 1);
      const int64_t alloc_size = 1 + sizeof(T) + item->get_deep_copy_size();
      pop_index++;
      pop_size += alloc_size;
      return OB_SUCCESS;
    } else {
      return OB_ITER_END;
    }
  }

  bool pop_round_finish() {
    return push_alloc != pop_alloc && push_size == pop_size;
  }

  void wait_switch_pop() {
    if (push_alloc != pop_alloc && push_size == pop_size) {
      dispatch_data_[pop_alloc_id].reset();
      pop_alloc->reset();
      push_size = push_used;
      pop_size = 0;
      pop_index = 0;
      pop_alloc_id = (pop_alloc_id + 1) % 2;
      pop_alloc = allocator_[pop_alloc_id];
    } else if (is_finished) {
      dispatch_data_[pop_alloc_id].reset();
      pop_alloc->reset();
    }
  }

  void reset() {
    allocator_[0]->reset();
    allocator_[1]->reset();
    dispatch_data_[0].reset();
    dispatch_data_[1].reset();
  }

  int64_t get_push_size() {
    if (push_alloc != pop_alloc)
      return push_size + push_used;
    else
      return push_size;
  }

  int64_t get_pop_size() { return pop_size; }

  TO_STRING_KV(K(is_finished), K(push_used), K(push_size), K(pop_size),
               K(pop_index), K(push_alloc_id), K(pop_alloc_id), KP(push_alloc),
               KP(pop_alloc), K(buf_mem_limit));

protected:
  // 线程安全
  // 多个解析线程添加并使用 push_allocator 分配
  // 同时会将分配成功的 buf 放入对应的 dispatch_data_[push_alloc_id]
  // 之后其他各个线程在无锁的情况下对相应的 buf 进行初始化
  // 注意，pop_item 可能会取出未初始化的 buf 的情况
  void *alloc(int64_t size) {
    LockGuard guard(lock);
    char *buf = NULL;
    if (size > buf_mem_limit) {
      STORAGE_LOG(WARN,
                  "invalid item size, must not larger than buf memory limit",
                  K(size), K(buf_mem_limit));
    } else if (push_used + size > buf_mem_limit) {
      // 重新添加时发现超过容量，必须分配完这次再切换
      buf = static_cast<char *>(push_alloc->alloc(size));
      if (OB_ISNULL(buf)) {
        STORAGE_LOG(WARN, "fail to alloc memory", K(size), K(buf_mem_limit));
      } else {
        buf[0] = 0;
        dispatch_data_[push_alloc_id].push_back((T *)buf);
        push_used += size;
        wait_switch_push();
        push_size = push_used;
        push_used = 0;
        push_alloc_id = (push_alloc_id + 1) % 2;
        push_alloc = allocator_[push_alloc_id];
      }
    } else {
      buf = static_cast<char *>(push_alloc->alloc(size));
      if (OB_ISNULL(buf)) {
        STORAGE_LOG(WARN, "fail to alloc memory", K(size), K(buf_mem_limit));
      } else {
        buf[0] = 0;
        dispatch_data_[push_alloc_id].push_back((T *)buf);
        push_used += size;
        if (push_alloc == pop_alloc)
          push_size = push_used;
      }
    }
    return buf;
  }

  void wait_switch_push() {
    common::ObArenaAllocator *new_push_alloc =
        allocator_[(push_alloc_id + 1) % 2];
    while (new_push_alloc == pop_alloc)
      usleep(push_sleep_time);
  }

protected:
  bool is_finished;
  int64_t push_used;
  int64_t push_size;
  int64_t pop_size;
  int pop_index;
  common::ObArenaAllocator *allocator_[2];
  DatumRowVector dispatch_data_[2];
  int push_alloc_id;
  common::ObArenaAllocator *push_alloc;
  int pop_alloc_id;
  common::ObArenaAllocator *pop_alloc;
  Lock lock;
  int64_t buf_mem_limit;
  uint64_t push_sleep_time;
  uint64_t pop_sleep_time;
};

template <typename T>
class ObMemoryFragmentIterator : public ObFragmentIterator<T> {
public:
  ObMemoryFragmentIterator();
  virtual ~ObMemoryFragmentIterator();
  int init(common::ObVector<T *> &item_list);
  virtual int get_next_item(const T *&item);
  virtual int clean_up() { return common::OB_SUCCESS; }

private:
  bool is_inited_;
  int64_t curr_item_index_;
  common::ObVector<T *> *item_list_;
};

template <typename T>
ObMemoryFragmentIterator<T>::ObMemoryFragmentIterator()
    : is_inited_(false), curr_item_index_(0), item_list_(NULL) {}

template <typename T>
ObMemoryFragmentIterator<T>::~ObMemoryFragmentIterator() {}

template <typename T>
int ObMemoryFragmentIterator<T>::init(common::ObVector<T *> &item_list) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMemoryFragmentIterator has been inited twice", K(ret));
  } else {
    item_list_ = &item_list;
    is_inited_ = true;
  }
  return ret;
}

template <typename T>
int ObMemoryFragmentIterator<T>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemoryFragmentIterator has not been inited", K(ret));
  } else if (curr_item_index_ >= item_list_->size()) {
    ret = common::OB_ITER_END;
  } else {
    item = (*item_list_)[curr_item_index_];
    ++curr_item_index_;
  }
  return ret;
}

template <typename T, typename Compare> class ObMemorySortRound {
public:
  typedef ObExternalSortRound<T, Compare> ExternalSortRound;
  ObMemorySortRound();
  virtual ~ObMemorySortRound();
  int init(const int64_t mem_limit, const int64_t expire_timestamp,
           Compare *compare, ExternalSortRound *next_round);
  int add_item(const T &item);
  int add_item_reuse(const T &item);
  int build_fragment();
  int build_fragment_reuse();
  int build_fragment_nosort();
  virtual int get_next_item(const T *&item);
  int finish();
  int finish_reuse();
  bool is_in_memory() const { return is_in_memory_; }
  bool has_data() const { return has_data_; }
  void reset();
  int transfer_sorted_fragment_iter(ExternalSortRound &dest_round);
  int transfer_final_sorted_fragment_iter(ExternalSortRound &dest_round);
  void set_dispatch_queue(DispatchQueue<T> *dispatch_queue) {
    dispatch_queue_ = dispatch_queue;
  }
  TO_STRING_KV(K(is_inited_), K(is_in_memory_), K(has_data_), K(buf_mem_limit_),
               K(expire_timestamp_), KP(next_round_), KP(compare_), KP(iter_));

private:
  int build_iterator();

private:
  bool is_inited_;
  bool is_in_memory_;
  bool has_data_;
  int64_t buf_mem_limit_;
  int64_t expire_timestamp_;
  ExternalSortRound *next_round_;
  common::ObArenaAllocator allocator_;
  common::ObVector<T *> item_list_;
  Compare *compare_;
  ObMemoryFragmentIterator<T> *iter_;
  DispatchQueue<T> *dispatch_queue_;
};

template <typename T, typename Compare>
ObMemorySortRound<T, Compare>::ObMemorySortRound()
    : is_inited_(false), is_in_memory_(false), has_data_(false),
      buf_mem_limit_(0), expire_timestamp_(0), next_round_(NULL),
      allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER,
                 common::OB_MALLOC_BIG_BLOCK_SIZE),
      item_list_(NULL, common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
      compare_(NULL), iter_(NULL) {}

template <typename T, typename Compare>
ObMemorySortRound<T, Compare>::~ObMemorySortRound() {}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::init(const int64_t mem_limit,
                                        const int64_t expire_timestamp,
                                        Compare *compare,
                                        ExternalSortRound *next_round) {
  int ret = common::OB_SUCCESS;
  allocator_.set_tenant_id(MTL_ID());
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMemorySortRound has been inited", K(ret));
  } else if (mem_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT ||
             NULL == compare || NULL == next_round ||
             !next_round->is_inited()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(mem_limit), KP(compare),
                KP(next_round), "next round inited", next_round->is_inited());
  } else {
    is_inited_ = true;
    is_in_memory_ = false;
    has_data_ = false;
    buf_mem_limit_ = mem_limit;
    expire_timestamp_ = expire_timestamp;
    compare_ = compare;
    next_round_ = next_round;
    iter_ = NULL;
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::add_item(const T &item) {
  int ret = common::OB_SUCCESS;
  const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
  char *buf = NULL;
  T *new_item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObMemorySortRound timeout", K(ret),
                K(expire_timestamp_));
  } else if (item_size > buf_mem_limit_) {
    ret = common::OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN,
                "invalid item size, must not larger than buf memory limit",
                K(ret), K(item_size), K(buf_mem_limit_));
  } else if (allocator_.used() + item_size > buf_mem_limit_ &&
             OB_FAIL(build_fragment())) {
    STORAGE_LOG(WARN, "fail to build fragment", K(ret));
  } else if (OB_ISNULL(buf =
                           static_cast<char *>(allocator_.alloc(item_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(item_size));
  } else if (OB_ISNULL(new_item = new (buf) T())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new item", K(ret));
  } else {
    int64_t buf_pos = sizeof(T);
    if (OB_FAIL(new_item->deep_copy(item, buf, item_size, buf_pos))) {
      STORAGE_LOG(WARN, "fail to deep copy item", K(ret));
    } else if (OB_FAIL(item_list_.push_back(new_item))) {
      STORAGE_LOG(WARN, "fail to push back new item", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::add_item_reuse(const T &item) {
  int ret = common::OB_SUCCESS;
  const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "ObMemorySortRound timeout", K(ret),
                K(expire_timestamp_));
  } else if (OB_FAIL(item_list_.push_back(&item))) {
    STORAGE_LOG(WARN, "fail to push back new item", K(ret));
  } else {
    if (dispatch_queue_->pop_round_finish() &&
        OB_FAIL(build_fragment_reuse())) {
      STORAGE_LOG(WARN, "fail to build fragment", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::build_fragment() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (item_list_.size() > 0) {
    int64_t start = common::ObTimeUtility::current_time();
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      ret = compare_->result_code_;
    } else {
      const int64_t sort_fragment_time =
          common::ObTimeUtility::current_time() - start;
      STORAGE_LOG(INFO, "ObMemorySortRound", K(sort_fragment_time));
    }

    start = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
      if (OB_FAIL(next_round_->add_item(*item_list_.at(i)))) {
        STORAGE_LOG(WARN, "fail to add item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round_->build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      } else {
        const int64_t write_fragment_time =
            common::ObTimeUtility::current_time() - start;
        STORAGE_LOG(INFO, "ObMemorySortRound", K(write_fragment_time));
        item_list_.reset();
        allocator_.reuse();
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::build_fragment_reuse() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (item_list_.size() > 0) {
    int64_t start = common::ObTimeUtility::current_time();
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      ret = compare_->result_code_;
    } else {
      const int64_t sort_fragment_time =
          common::ObTimeUtility::current_time() - start;
      STORAGE_LOG(INFO, "ObMemorySortRound", K(sort_fragment_time));
    }

    start = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
      if (OB_FAIL(next_round_->add_item(*item_list_.at(i)))) {
        STORAGE_LOG(WARN, "fail to add item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round_->build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      } else {
        const int64_t write_fragment_time =
            common::ObTimeUtility::current_time() - start;
        STORAGE_LOG(INFO, "ObMemorySortRound", K(write_fragment_time));
        item_list_.reset();
        dispatch_queue_->wait_switch_pop();
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::build_fragment_nosort() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (item_list_.size() > 0) {
    int64_t start = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list_.size(); ++i) {
      if (OB_FAIL(next_round_->add_item(*item_list_.at(i)))) {
        STORAGE_LOG(WARN, "fail to add item", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_round_->build_fragment())) {
        STORAGE_LOG(WARN, "fail to build fragment", K(ret));
      } else {
        const int64_t write_fragment_time =
            common::ObTimeUtility::current_time() - start;
        STORAGE_LOG(INFO, "ObMemorySortRound", K(write_fragment_time));
        item_list_.reset();
        allocator_.reuse();
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::finish() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (0 == item_list_.size()) {
    has_data_ = false;
  } else if (0 == next_round_->get_fragment_count()) {
    is_in_memory_ = true;
    has_data_ = true;
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      STORAGE_LOG(WARN, "fail to sort item list", K(ret));
    }
  } else {
    is_in_memory_ = false;
    has_data_ = true;
    if (OB_FAIL(build_fragment())) {
      STORAGE_LOG(WARN, "fail to build fragment", K(ret));
    } else if (OB_FAIL(next_round_->finish_write())) {
      STORAGE_LOG(WARN, "fail to do next round finish write", K(ret));
    } else {
      item_list_.reset();
      allocator_.reset();
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::finish_reuse() {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (0 == item_list_.size()) {
    has_data_ = false;
  } else if (0 == next_round_->get_fragment_count()) {
    is_in_memory_ = true;
    has_data_ = true;
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      STORAGE_LOG(WARN, "fail to sort item list", K(ret));
    }
  } else {
    is_in_memory_ = false;
    has_data_ = true;
    if (OB_FAIL(build_fragment_reuse())) {
      STORAGE_LOG(WARN, "fail to build fragment reuse", K(ret));
    } else if (OB_FAIL(next_round_->finish_write())) {
      STORAGE_LOG(WARN, "fail to do next round finish write", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::build_iterator() {
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (OB_ISNULL(
                 buf = allocator_.alloc(sizeof(ObMemoryFragmentIterator<T>)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for ObMemoryFragmentIterator",
                K(ret));
  } else if (OB_ISNULL(iter_ = new (buf) ObMemoryFragmentIterator<T>())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new ObMemoryFragmentIterator", K(ret));
  } else if (OB_FAIL(iter_->init(item_list_))) {
    STORAGE_LOG(WARN, "fail to init iterator", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (NULL == iter_) {
    if (OB_FAIL(build_iterator())) {
      STORAGE_LOG(WARN, "fail to build iterator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == iter_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, iter must not be null", K(ret),
                  KP(iter_));
    } else if (OB_FAIL(iter_->get_next_item(item))) {
      if (ret != OB_ITER_END) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
void ObMemorySortRound<T, Compare>::reset() {
  is_inited_ = false;
  is_in_memory_ = false;
  buf_mem_limit_ = 0;
  expire_timestamp_ = 0;
  next_round_ = NULL;
  allocator_.reset();
  item_list_.reset();
  compare_ = NULL;
  iter_ = NULL;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::transfer_sorted_fragment_iter(
    ExternalSortRound &dest_round) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (!is_in_memory()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "ObMemorySortRound has not data", K(ret));
  } else if (NULL == iter_ && OB_FAIL(build_fragment_nosort())) {
    STORAGE_LOG(WARN, "fail to build fragment", K(ret));
  } else if (OB_FAIL(next_round_->transfer_sorted_fragment_iter(dest_round))) {
    STORAGE_LOG(WARN, "fail to add transfer sorted fragment iter", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObMemorySortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    ExternalSortRound &dest_round) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMemorySortRound has not been inited", K(ret));
  } else if (!is_in_memory()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "ObMemorySortRound has not data", K(ret));
  } else if (NULL == iter_ && OB_FAIL(build_iterator())) {
    STORAGE_LOG(WARN, "fail to build iterator", K(ret));
  } else if (OB_FAIL(dest_round.add_fragment_iter(iter_))) {
    STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
  } else {
    iter_ = NULL;
  }
  return ret;
}

template <typename T, typename Compare> class ObExternalSort {
public:
  typedef ObMemorySortRound<T, Compare> MemorySortRound;
  typedef ObExternalSortRound<T, Compare> ExternalSortRound;
  ObExternalSort();
  virtual ~ObExternalSort();
  int init(const int64_t mem_limit, const int64_t file_buf_size,
           const int64_t expire_timestamp, const uint64_t tenant_id,
           Compare *compare, const int count,
           const ObArray<ObColDesc> &col_descs);
  int add_item(const T &item);
  int add_item_reuse(const T &item);
  int do_sort(const bool final_merge);
  int do_sort_reuse(const bool final_merge);
  int get_next_item(const T *&item);
  void clean_up();
  int add_fragment_iter(ObFragmentIterator<T> *iter);
  int transfer_sorted_fragment_iter(ObExternalSort<T, Compare> &merge_sorter);
  int transfer_final_sorted_fragment_iter(
      ObExternalSort<T, Compare> &merge_sorter);
  int get_current_round(ExternalSortRound *&round);
  void set_dispatch_queue(DispatchQueue<T> *dispatch_queue) {
    memory_sort_round_.set_dispatch_queue(dispatch_queue);
  }
  TO_STRING_KV(K(is_inited_), K(file_buf_size_), K(buf_mem_limit_),
               K(expire_timestamp_), K(merge_count_per_round_), KP(tenant_id_),
               KP(compare_));

private:
  static const int64_t EXTERNAL_SORT_ROUND_CNT = 2;
  bool is_inited_;
  int count_;
  ObArray<ObColDesc> col_descs_;
  int64_t file_buf_size_;
  int64_t buf_mem_limit_;
  int64_t expire_timestamp_;
  int64_t merge_count_per_round_;
  Compare *compare_;
  MemorySortRound memory_sort_round_;
  ExternalSortRound sort_rounds_[EXTERNAL_SORT_ROUND_CNT];
  ExternalSortRound *curr_round_;
  ExternalSortRound *next_round_;
  bool is_empty_;
  uint64_t tenant_id_;
};

template <typename T, typename Compare>
ObExternalSort<T, Compare>::ObExternalSort()
    : is_inited_(false), file_buf_size_(0), buf_mem_limit_(0),
      expire_timestamp_(0), merge_count_per_round_(0), compare_(NULL),
      memory_sort_round_(), curr_round_(NULL), next_round_(NULL),
      is_empty_(true), tenant_id_(common::OB_INVALID_ID) {}

template <typename T, typename Compare>
ObExternalSort<T, Compare>::~ObExternalSort() {}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::init(const int64_t mem_limit,
                                     const int64_t file_buf_size,
                                     const int64_t expire_timestamp,
                                     const uint64_t tenant_id, Compare *compare,
                                     const int count,
                                     const ObArray<ObColDesc> &col_descs) {
  int ret = common::OB_SUCCESS;
  int64_t macro_block_size = ObExternalSortConstant::ALIGN_SIZE;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObExternalSort has already been inited", K(ret));
  } else if (mem_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT ||
             file_buf_size % DIO_ALIGN_SIZE != 0 ||
             file_buf_size < macro_block_size ||
             common::OB_INVALID_ID == tenant_id || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(mem_limit),
                K(file_buf_size), KP(compare));
  } else {
    file_buf_size_ = common::lower_align(file_buf_size, macro_block_size);
    buf_mem_limit_ = mem_limit;
    expire_timestamp_ = expire_timestamp;
    merge_count_per_round_ = buf_mem_limit_ / file_buf_size_ / 2;
    compare_ = compare;
    tenant_id_ = tenant_id;
    curr_round_ = &sort_rounds_[0];
    next_round_ = &sort_rounds_[1];
    is_empty_ = true;
    if (merge_count_per_round_ <
        ObExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument, invalid memory limit", K(ret),
                  K(buf_mem_limit_), K(file_buf_size_),
                  K(merge_count_per_round_));
    } else if (OB_FAIL(curr_round_->init(merge_count_per_round_, file_buf_size_,
                                         expire_timestamp, tenant_id_, compare_,
                                         count, col_descs))) {
      STORAGE_LOG(WARN, "fail to init current sort round", K(ret));
    } else if (OB_FAIL(memory_sort_round_.init(buf_mem_limit_, expire_timestamp,
                                               compare_, curr_round_))) {
      STORAGE_LOG(WARN, "fail to init memory sort round", K(ret));
    } else {
      is_inited_ = true;
      count_ = count;
      col_descs_ = col_descs;
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::add_item(const T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.add_item(item))) {
    STORAGE_LOG(WARN, "fail to add item in memory sort round", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::add_item_reuse(const T &item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.add_item_reuse(item))) {
    STORAGE_LOG(WARN, "fail to add item in memory sort round", K(ret));
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::do_sort(const bool final_merge) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.finish())) {
    STORAGE_LOG(WARN, "fail to finish memory sort round", K(ret));
  } else if (memory_sort_round_.has_data() &&
             memory_sort_round_.is_in_memory()) {
    STORAGE_LOG(INFO, "all data sorted in memory");
    is_empty_ = false;
  } else if (0 == curr_round_->get_fragment_count()) {
    is_empty_ = true;
    ret = common::OB_SUCCESS;
  } else {
    // final_merge = true is for performance optimization, the count of
    // fragments is reduced to lower than merge_count_per_round, then the last
    // round of merge this fragment is skipped
    const int64_t final_round_limit = final_merge ? merge_count_per_round_ : 1;
    int64_t round_id = 1;
    is_empty_ = false;
    while (OB_SUCC(ret) &&
           curr_round_->get_fragment_count() > final_round_limit) {
      const int64_t start_time = common::ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "do sort start round", K(round_id));
      if (OB_FAIL(next_round_->init(merge_count_per_round_, file_buf_size_,
                                    expire_timestamp_, tenant_id_, compare_,
                                    count_, col_descs_))) {
        STORAGE_LOG(WARN, "fail to init next sort round", K(ret));
      } else if (OB_FAIL(curr_round_->do_merge(*next_round_))) {
        STORAGE_LOG(WARN, "fail to do merge fragments of current round",
                    K(ret));
      } else if (OB_FAIL(curr_round_->clean_up())) {
        STORAGE_LOG(WARN, "fail to do clean up of current round", K(ret));
      } else {
        std::swap(curr_round_, next_round_);
        const int64_t round_cost_time =
            common::ObTimeUtility::current_time() - start_time;
        STORAGE_LOG(INFO, "do sort end round", K(round_id), K(round_cost_time));
        ++round_id;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(curr_round_->build_merger())) {
        STORAGE_LOG(WARN, "fail to build merger", K(ret));
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::do_sort_reuse(const bool final_merge) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.finish_reuse())) {
    STORAGE_LOG(WARN, "fail to finish memory sort round", K(ret));
  } else if (memory_sort_round_.has_data() &&
             memory_sort_round_.is_in_memory()) {
    STORAGE_LOG(INFO, "all data sorted in memory");
    is_empty_ = false;
  } else if (0 == curr_round_->get_fragment_count()) {
    is_empty_ = true;
    ret = common::OB_SUCCESS;
  } else {
    // final_merge = true is for performance optimization, the count of
    // fragments is reduced to lower than merge_count_per_round, then the last
    // round of merge this fragment is skipped
    const int64_t final_round_limit = final_merge ? merge_count_per_round_ : 1;
    int64_t round_id = 1;
    is_empty_ = false;
    while (OB_SUCC(ret) &&
           curr_round_->get_fragment_count() > final_round_limit) {
      const int64_t start_time = common::ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "do sort start round", K(round_id));
      if (OB_FAIL(next_round_->init(merge_count_per_round_, file_buf_size_,
                                    expire_timestamp_, tenant_id_, compare_,
                                    count_, col_descs_))) {
        STORAGE_LOG(WARN, "fail to init next sort round", K(ret));
      } else if (OB_FAIL(curr_round_->do_merge(*next_round_))) {
        STORAGE_LOG(WARN, "fail to do merge fragments of current round",
                    K(ret));
      } else if (OB_FAIL(curr_round_->clean_up())) {
        STORAGE_LOG(WARN, "fail to do clean up of current round", K(ret));
      } else {
        std::swap(curr_round_, next_round_);
        const int64_t round_cost_time =
            common::ObTimeUtility::current_time() - start_time;
        STORAGE_LOG(INFO, "do sort end round", K(round_id), K(round_cost_time));
        ++round_id;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(curr_round_->build_merger())) {
        STORAGE_LOG(WARN, "fail to build merger", K(ret));
      }
    }
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::get_next_item(const T *&item) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_ITER_END;
  } else if (memory_sort_round_.has_data() &&
             memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(memory_sort_round_.get_next_item(item))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next item", K(ret));
      }
    }
  } else if (OB_FAIL(curr_round_->get_next_item(item))) {
    if (common::OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template <typename T, typename Compare>
void ObExternalSort<T, Compare>::clean_up() {
  int tmp_ret = common::OB_SUCCESS;
  is_inited_ = false;
  count_ = 0;
  file_buf_size_ = 0;
  buf_mem_limit_ = 0;
  expire_timestamp_ = 0;
  merge_count_per_round_ = 0;
  compare_ = NULL;
  memory_sort_round_.reset();
  curr_round_ = NULL;
  next_round_ = NULL;
  is_empty_ = true;
  STORAGE_LOG(INFO, "do external sort clean up");
  for (int64_t i = 0; i < EXTERNAL_SORT_ROUND_CNT; ++i) {
    // ignore ret
    if (sort_rounds_[i].is_inited() &&
        common::OB_SUCCESS != (tmp_ret = sort_rounds_[i].clean_up())) {
      STORAGE_LOG(WARN, "fail to clean up sort rounds", K(tmp_ret), K(i));
    }
  }
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::add_fragment_iter(ObFragmentIterator<T> *iter) {
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(curr_round_->add_fragment_iter(iter))) {
    STORAGE_LOG(WARN, "fail to add fragment iter");
  } else {
    is_empty_ = false;
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::get_current_round(
    ExternalSortRound *&curr_round) {
  int ret = common::OB_SUCCESS;
  curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (NULL == curr_round_) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid current round", K(ret), KP(curr_round_));
  } else {
    curr_round = curr_round_;
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::transfer_sorted_fragment_iter(
    ObExternalSort<T, Compare> &merge_sorter) {
  int ret = common::OB_SUCCESS;
  ExternalSortRound *curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_SUCCESS;
  } else if (OB_FAIL(merge_sorter.get_current_round(curr_round))) {
    STORAGE_LOG(WARN, "fail to get current round", K(ret));
  } else if (NULL == curr_round) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner state", K(ret), KP(curr_round));
  } else if (memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(
            memory_sort_round_.transfer_sorted_fragment_iter(*curr_round))) {
      STORAGE_LOG(WARN, "fail to transfer final sorted fragment iterator",
                  K(ret));
    } else {
      merge_sorter.is_empty_ = false;
    }
  } else if (OB_FAIL(curr_round_->transfer_sorted_fragment_iter(*curr_round))) {
    STORAGE_LOG(WARN, "fail to get transfer sorted fragment iterator", K(ret));
  } else {
    merge_sorter.is_empty_ = false;
  }
  return ret;
}

template <typename T, typename Compare>
int ObExternalSort<T, Compare>::transfer_final_sorted_fragment_iter(
    ObExternalSort<T, Compare> &merge_sorter) {
  int ret = common::OB_SUCCESS;
  ExternalSortRound *curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_SUCCESS;
  } else if (OB_FAIL(merge_sorter.get_current_round(curr_round))) {
    STORAGE_LOG(WARN, "fail to get current round", K(ret));
  } else if (NULL == curr_round) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner state", K(ret), KP(curr_round));
  } else if (memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(memory_sort_round_.transfer_final_sorted_fragment_iter(
            *curr_round))) {
      STORAGE_LOG(WARN, "fail to transfer final sorted fragment iterator",
                  K(ret));
    } else {
      merge_sorter.is_empty_ = false;
    }
  } else if (OB_FAIL(curr_round_->transfer_final_sorted_fragment_iter(
                 *curr_round))) {
    STORAGE_LOG(WARN, "fail to get transfer sorted fragment iterator", K(ret));
  } else {
    merge_sorter.is_empty_ = false;
  }
  return ret;
}

} // namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_
