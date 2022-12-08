#ifndef OB_ENCODE_DECODE
#define OB_ENCODE_DECODE
#include "lib/compress/lz4/ob_lz4_compressor.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_vector.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
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
typedef oceanbase::blocksstable::ObStorageDatum Datum;

// 输入为一个 Datum，输出序列化的结果到 dest_buf，修改 pos
// cap 为 dest_buf 的容量
// 这里不做 buf 容量的检验，如果序列化的数据超出容量，则会 crash
typedef int (*encode_func)(const Datum &src_datum, char *dest_buf, int64_t &pos,
                           const int64_t cap);

// 输入编码后的全部数据 src_buf
// 输出解码后的全部数据 dest_buf
typedef int (*decode_to_buf)(const char *src_buf, int64_t &src_pos,
                             const int64_t src_cap, char *dest_buf,
                             int64_t &dest_pos, const int64_t dest_cap);

// 输入解码后的全部数据，以及当前解码所处的文件
// 解码一条数据到 Datum，修改 pos
typedef int (*decode_func)(const char *src_buf, int64_t &pos, const int64_t cap,
                           Datum &dest_datum);

static int default_encode(const Datum &src_datum, char *dest_buf, int64_t &pos,
                          const int64_t cap) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(src_datum.serialize(dest_buf, cap, pos))) {
    STORAGE_LOG(WARN, "fail to serialize item", K(ret));
  }
  return ret;
};

static int default_decode_tobuf(const char *src_buf, int64_t &src_pos,
                                const int64_t src_cap, char *dest_buf,
                                int64_t &dest_pos, const int64_t dest_cap) {
  int ret = OB_SUCCESS;
  memcpy(dest_buf + dest_pos, src_buf + src_pos, src_cap - src_pos);
  return ret;
};

static int default_decode(const char *src_buf, int64_t &pos, const int64_t cap,
                          Datum &dest_datum) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest_datum.deserialize(src_buf, cap, pos))) {
    STORAGE_LOG(WARN, "fail to serialize item", K(ret));
  }
  return ret;
}

inline uint32_t ZigZagEncode32(int32_t n) {
  // Note:  the right-shift must be arithmetic
  return (n << 1) ^ (n >> 31);
}

inline int32_t ZigZagDecode32(uint32_t n) {
  return (n >> 1) ^ -static_cast<int32_t>(n & 1);
}

inline uint64_t ZigZagEncode64(int64_t n) {
  // Note:  the right-shift must be arithmetic
  return (n << 1) ^ (n >> 63);
}

inline int64_t ZigZagDecode64(uint64_t n) {
  return (n >> 1) ^ -static_cast<int64_t>(n & 1);
}

static const int B = 0x80;

static int int_encode64(const Datum &src_datum, char *dest_buf, int64_t &pos,
                        const int64_t cap) {
  int ret = OB_SUCCESS;
  int64_t n_ = src_datum.get_int();
  uint64_t n = ZigZagEncode64(n_);
  uint8_t *ptr = reinterpret_cast<uint8_t *>(&dest_buf[pos]);
  while (n >= B) {
    *(ptr++) = n | B;
    n >>= 7;
  }
  *(ptr++) = static_cast<uint8_t>(n);
  pos += (reinterpret_cast<char *>(ptr) - dest_buf);
  return ret;
}

static int int_encode32(const Datum &src_datum, char *dest_buf, int64_t &pos,
                        const int64_t cap) {
  int ret = OB_SUCCESS;
  int32_t n_ = src_datum.get_int32();
  uint32_t n = ZigZagEncode32(n_);
  uint8_t *ptr = reinterpret_cast<uint8_t *>(&dest_buf[pos]);
  if (n < (1 << 7)) {
    *(ptr++) = n;
  } else if (n < (1 << 14)) {
    *(ptr++) = n | B;
    *(ptr++) = n >> 7;
  } else if (n < (1 << 21)) {
    *(ptr++) = n | B;
    *(ptr++) = (n >> 7) | B;
    *(ptr++) = n >> 14;
  } else if (n < (1 << 28)) {
    *(ptr++) = n | B;
    *(ptr++) = (n >> 7) | B;
    *(ptr++) = (n >> 14) | B;
    *(ptr++) = n >> 21;
  } else {
    *(ptr++) = n | B;
    *(ptr++) = (n >> 7) | B;
    *(ptr++) = (n >> 14) | B;
    *(ptr++) = (n >> 21) | B;
    *(ptr++) = n >> 28;
  }
  pos += (reinterpret_cast<char *>(ptr) - dest_buf);
  return ret;
}

static int int_decode64_to_buf(const char *src_buf, int64_t &src_pos,
                               const int64_t src_cap, char *dest_buf,
                               int64_t &dest_pos, const int64_t dest_cap) {
  int ret = OB_SUCCESS;
  int64_t temp_dest_pos = dest_pos;
  while (src_pos < src_cap) {
    uint64_t n_ = 0;
    for (uint32_t shift = 0; shift <= 63; shift += 7) {
      uint64_t byte = *(reinterpret_cast<const uint8_t *>(&src_buf[src_pos]));
      src_pos++;
      if (byte & B) {
        n_ |= ((byte & 127) << shift);
      } else {
        n_ |= (byte << shift);
        break;
      }
    }
    int64_t n = ZigZagDecode64(n_);
    memset(dest_buf + temp_dest_pos, n, sizeof(int64_t));
    temp_dest_pos += 8;
  }
  return ret;
}

static int int_decode64(const char *src_buf, int64_t &pos, const int64_t cap,
                        Datum &dest_datum) {
  int ret = OB_SUCCESS;
  dest_datum.from_buf_enhance(src_buf + pos, 8);
  pos += 8;
  return ret;
}

static int int_decode32_to_buf(const char *src_buf, int64_t &src_pos,
                               const int64_t src_cap, char *dest_buf,
                               int64_t &dest_pos, const int64_t dest_cap) {
  int ret = OB_SUCCESS;
  int64_t temp_dest_pos = dest_pos;
  while (src_pos < src_cap) {
    uint32_t n_ = 0;
    uint32_t byte = *(reinterpret_cast<const uint8_t *>(&src_buf[src_pos]));
    // 首先进行一次特判
    if ((byte & 128) == 0) {
      src_pos++;
      int32_t n = ZigZagDecode32(byte);
      memset(dest_buf + temp_dest_pos, n, sizeof(int32_t));
    } else {
      for (uint32_t shift = 0; shift <= 28; shift += 7) {
        byte = *(reinterpret_cast<const uint8_t *>(&src_buf[src_pos]));
        src_pos++;
        if (byte & B) {
          n_ |= ((byte & 127) << shift);
        } else {
          n_ |= (byte << shift);
          break;
        }
      }
      int32_t n = ZigZagDecode32(n_);
      memset(dest_buf + temp_dest_pos, n, sizeof(int32_t));
    }
    temp_dest_pos += 4;
  }
  return ret;
}

static int int_decode32(const char *src_buf, int64_t &pos, const int64_t cap,
                        Datum &dest_datum) {
  int ret = OB_SUCCESS;
  // FIXME: 如果这里数据不对，那么需要把前面的 memset size 更改为 8
  dest_datum.from_buf_enhance(src_buf + pos, 8);
  pos += 4;
  return ret;
}

} // namespace sql
} // namespace oceanbase

#endif