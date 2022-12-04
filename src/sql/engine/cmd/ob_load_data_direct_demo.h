#pragma once

#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/file/ob_file.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/thread.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"

#include <atomic>
#include <thread>

namespace oceanbase {
namespace sql {

static const int64_t MAX_RECORD_SIZE = (1LL << 12);         // 4K
static const int64_t MEM_BUFFER_SIZE = (1LL << 20) * 576LL; // 576M
static const int64_t FILE_BUFFER_SIZE = (2LL << 20);        // 2M
static const int64_t SAMPLING_NUM = (1LL << 20);            // 1M
static const int64_t BUFFER_NUM = (4LL << 20);              // 4M

static const uint64_t SLEEP_TIME = 800000;

class ObLoadDataBuffer {
public:
  ObLoadDataBuffer();
  ~ObLoadDataBuffer();
  void reuse();
  void reset();
  int create(int64_t capacity);
  int squash();
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE char *end() const { return data_ + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
  OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
  OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }

  bool is_end=false;

private:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t capacity_;
};

class ObLoadSequentialFileReader {
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  int read_next_buffer(ObLoadDataBuffer &buffer);
  int set_offset_end(int64_t offset, int64_t end);

private:
  char buf_[MAX_RECORD_SIZE];
  common::ObFileReader file_reader_;
  int64_t offset_;
  int64_t end_;
  bool is_read_end_;
};

class ObLoadCSVPaser {
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);

private:
  struct UnusedRowHandler {
    int operator()(
        common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line) {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    }
  };

private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  ObCSVGeneralParser csv_parser_;
  common::ObNewRow row_;
  UnusedRowHandler unused_row_handler_;
  common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
  bool is_inited_;
};

class ObLoadDatumRow {
  OB_UNIS_VERSION(1);

public:
  ObLoadDatumRow();
  ~ObLoadDatumRow();
  void reset();
  int init(int64_t capacity);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len,
                int64_t &pos);
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
  DECLARE_TO_STRING;

public:
  common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

class ObLoadDatumRowCompare {
public:
  ObLoadDatumRowCompare();
  ~ObLoadDatumRowCompare();
  int init(int64_t rowkey_column_num,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
  int get_error_code() const { return result_code_; }

public:
  int result_code_;

private:
  int64_t rowkey_column_num_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  static thread_local blocksstable::ObDatumRowkey lhs_rowkey_;
  static thread_local blocksstable::ObDatumRowkey rhs_rowkey_;
  bool is_inited_;
};

class ObLoadRowCaster {
public:
  ObLoadRowCaster();
  ~ObLoadRowCaster();
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct>
               &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row,
                     const ObLoadDatumRow *&datum_row);

private:
  int init_column_schemas_and_idxs(
      const share::schema::ObTableSchema *table_schema,
      const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct>
          &field_or_var_list);
  int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                        const common::ObObj &obj,
                        blocksstable::ObStorageDatum &datum);

private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  common::ObArray<int64_t>
      column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  ObLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
  bool is_inited_;
};

class ObLoadExternalSort {
  typedef storage::DispatchQueue<ObLoadDatumRow> ObLoadDispatchQueue;
public:
  ObLoadExternalSort();
  ~ObLoadExternalSort();
  int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
           int64_t file_buf_size);
  int append_row(const ObLoadDatumRow &datum_row);
  int append_row_reuse(const ObLoadDatumRow &datum_row);
  int close();
  int get_next_row(const ObLoadDatumRow *&datum_row);
  void set_dispatch_queue(ObLoadDispatchQueue *dispatch_queue) {
    external_sort_.set_dispatch_queue(dispatch_queue);
  }

private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
  storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadSSTableWriter {
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  int init(const share::schema::ObTableSchema *table_schema);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
  int finish();
  int init_macro_block_writer(const int64_t parallel_idx);

private:
  int init_sstable_index_builder(
      const share::schema::ObTableSchema *table_schema);
  int init_data_store_desc(const share::schema::ObTableSchema *table_schema);
  int create_sstable();

private:
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  static thread_local blocksstable::ObMacroBlockWriter macro_block_writer_;
  static thread_local blocksstable::ObDatumRow datum_row_;
  static thread_local bool is_closed_;
  bool is_inited_;
  bool is_finished_;
  common::ObSpinLock lock_;
};

// 读取 csv 解析完的数据，然后分发到对应的外排
// 首先会对数据进行采样划分范围
// CSV->DISPATCH->EXTERNAL_SORT
class ObLoadDispatcher {
  typedef common::ObSpinLock Lock;
  typedef lib::ObLockGuard<Lock> LockGuard;
  typedef common::ObVector<ObLoadDatumRow *> LoadDatumRowVector;
  typedef storage::DispatchQueue<ObLoadDatumRow> ObLoadDispatchQueue;
public:
  // thread_num 指的是向 ObLoadDispatcher 提供数据的线程数量
  // dispatch_num 指的是 ObLoadDispatcher 分发到外排桶的数量
  ObLoadDispatcher(int thread_num, int dispatch_num);
  ~ObLoadDispatcher();
  int init(const ObTableSchema *table_schema);
  int append_row(const ObLoadDatumRow &datum_row);
  int get_next_row(int idx, const ObLoadDatumRow *&datum_row);
  int close(int idx);
  void error_close(int idx);
  ObLoadDispatchQueue *get_dispatch_queue(int idx);
  void debug_print();

private:
  int do_dispatch_all();
  int do_dispatch_one(const ObLoadDatumRow *item);
  int do_stat();

private:
  Lock lock_;
  bool is_inited_;
  int thread_num_;
  common::ObArray<bool> is_closed_;
  bool is_finished_;

  bool finish_smapling_;
  bool finish_smapling_stat_;

  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;

  std::atomic<int64_t> current_num_;
  LoadDatumRowVector smapling_data_;
  Lock smapling_data_lock_;

  int dispatch_num_;
  common::ObArray<ObLoadDatumRow *> dispatch_point_;
  common::ObArray<ObLoadDispatchQueue *> dispatch_queue_;
};

class ObLoadDataDirectDemo : public ObLoadDataBase {
public:
  ObLoadDataDirectDemo();
  virtual ~ObLoadDataDirectDemo();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
  int init(int64_t index, ObLoadDataStmt &load_stmt, int64_t offset,
           int64_t end, bool stage_process, ObLoadDispatcher *dispatcher,
           ObLoadSSTableWriter *sstable_writer = nullptr);

private:
  int inner_init_process(ObLoadDataStmt &load_stmt);
  int inner_init_load(ObLoadDataStmt &load_stmt);
  int do_process();
  int do_load();

private:
  int64_t index_;
  bool stage_process_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
  ObLoadCSVPaser csv_parser_;
  ObLoadRowCaster row_caster_;
  ObLoadDispatcher *dispatcher_;
  ObLoadExternalSort external_sort_;
  ObLoadSSTableWriter *sstable_writer_;
};

} // namespace sql
} // namespace oceanbase
