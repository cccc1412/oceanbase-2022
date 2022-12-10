#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
namespace oceanbase {
namespace sql {
using namespace blocksstable;
using namespace common;
using namespace lib;
using namespace observer;
using namespace share;
using namespace storage;
using namespace share::schema;

thread_local blocksstable::ObMacroBlockWriter
    ObLoadSSTableWriter::macro_block_writer_;
thread_local blocksstable::ObDatumRow ObLoadSSTableWriter::datum_row_;
thread_local bool ObLoadSSTableWriter::is_closed_ = false;

thread_local blocksstable::ObDatumRowkey ObLoadDatumRowCompare::lhs_rowkey_;
thread_local blocksstable::ObDatumRowkey ObLoadDatumRowCompare::rhs_rowkey_;
/**
 * ObLoadDataBuffer
 */

ObLoadDataBuffer::ObLoadDataBuffer()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA), data_(nullptr), begin_pos_(0),
      end_pos_(0), capacity_(0) {}

ObLoadDataBuffer::~ObLoadDataBuffer() { reset(); }

void ObLoadDataBuffer::reuse() {
  begin_pos_ = 0;
  end_pos_ = 0;
}

void ObLoadDataBuffer::reset() {
  allocator_.reset();
  data_ = nullptr;
  begin_pos_ = 0;
  end_pos_ = 0;
  capacity_ = 0;
}

int ObLoadDataBuffer::create(int64_t capacity) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != data_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      capacity_ = capacity;
    }
  }
  return ret;
}

int ObLoadDataBuffer::squash() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataBuffer not init", KR(ret), KP(this));
  } else {
    const int64_t data_size = get_data_size();
    if (data_size > 0) {
      MEMMOVE(data_, data_ + begin_pos_, data_size);
    }
    begin_pos_ = 0;
    end_pos_ = data_size;
  }
  return ret;
}

/**
 * ObLoadSequentialFileReader
 */

ObLoadSequentialFileReader::ObLoadSequentialFileReader()
    : offset_(0), is_read_end_(false) {}

ObLoadSequentialFileReader::~ObLoadSequentialFileReader() {}

int ObLoadSequentialFileReader::open(const ObString &filepath) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.open(filepath, false))) {
    LOG_WARN("fail to open file", KR(ret));
  }
  return ret;
}

int ObLoadSequentialFileReader::set_offset_end(int64_t offset, int64_t end) {
  int ret = OB_SUCCESS;

  int64_t read_size = 0;
  // memset(buf_, 0, MAX_RECORD_SIZE);
  if (OB_FAIL(file_reader_.pread(buf_, MAX_RECORD_SIZE, end, read_size))) {
    LOG_WARN("fail to do pread", KR(ret));
    goto out;
  } else {
    int64_t i = 0;
    while (i < read_size && buf_[i] != '\n') {
      i++;
      end++;
    }
    end++;
  }

  if (offset != 0) {
    read_size = 0;
    // memset(buf_, 0, MAX_RECORD_SIZE);
    if (OB_FAIL(file_reader_.pread(buf_, MAX_RECORD_SIZE, offset, read_size))) {
      LOG_WARN("fail to do pread", KR(ret));
      goto out;
    } else {
      int64_t i = 0;
      while (i < read_size && buf_[i] != '\n') {
        i++;
        offset++;
      }
      offset++;
    }
  }

  offset_ = offset;
  end_ = end;
out:
  return ret;
}

int ObLoadSequentialFileReader::read_next_buffer(ObLoadDataBuffer &buffer) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_reader_.is_opened())) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("file not opened", KR(ret));
  } else if (is_read_end_) {
    ret = OB_ITER_END;
  } else if (OB_LIKELY(buffer.get_remain_size() > 0)) {
    const int64_t buffer_remain_size = buffer.get_remain_size();
    const int64_t read_size_ = min(buffer_remain_size, end_ - offset_);
    int64_t read_size = 0;
    if (OB_FAIL(
            file_reader_.pread(buffer.end(), read_size_, offset_, read_size))) {
      LOG_WARN("fail to do pread", KR(ret));
    } else if (read_size == 0) {
      is_read_end_ = true;
      ret = OB_ITER_END;
    } else {
      offset_ += read_size;
      buffer.produce(read_size);
    }
  }
  return ret;
}

/**
 * ObLoadCSVPaser
 */

ObLoadCSVPaser::ObLoadCSVPaser()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA), collation_type_(CS_TYPE_INVALID),
      is_inited_(false) {}

ObLoadCSVPaser::~ObLoadCSVPaser() { reset(); }

void ObLoadCSVPaser::reset() {
  allocator_.reset();
  collation_type_ = CS_TYPE_INVALID;
  row_.reset();
  err_records_.reset();
  is_inited_ = false;
}

int ObLoadCSVPaser::init(const ObDataInFileStruct &format, int64_t column_count,
                         ObCollationType collation_type) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadCSVPaser init twice", KR(ret), KP(this));
  } else if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    ObObj *objs = nullptr;
    if (OB_ISNULL(objs = static_cast<ObObj *>(
                      allocator_.alloc(sizeof(ObObj) * column_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (objs) ObObj[column_count];
      row_.cells_ = objs;
      row_.count_ = column_count;
      collation_type_ = collation_type;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadCSVPaser::get_next_row(ObLoadDataBuffer &buffer,
                                 const ObNewRow *&row) {
  int ret = OB_SUCCESS;
  row = nullptr;
  if (buffer.empty()) {
    ret = OB_ITER_END;
  } else {
    const char *str = buffer.begin();
    const char *end = buffer.end();
    int64_t nrows = 1;
    if (OB_FAIL(csv_parser_.scan(str, end, nrows, nullptr, nullptr,
                                 unused_row_handler_, err_records_, false))) {
      LOG_WARN("fail to scan buffer", KR(ret));
    } else if (OB_UNLIKELY(!err_records_.empty())) {
      ret = err_records_.at(0).err_code;
      LOG_WARN("fail to parse line", KR(ret));
    } else if (0 == nrows) {
      ret = OB_ITER_END;
    } else {
      buffer.consume(str - buffer.begin());
      const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
          csv_parser_.get_fields_per_line();
      for (int64_t i = 0; i < row_.count_; ++i) {
        const ObCSVGeneralParser::FieldValue &str_v =
            field_values_in_file.at(i);
        ObObj &obj = row_.cells_[i];
        if (str_v.is_null_) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          obj.set_collation_type(collation_type_);
        }
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * ObLoadDatumRow
 */

ObLoadDatumRow::ObLoadDatumRow()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA), capacity_(0), count_(0),
      datums_(nullptr) {}

ObLoadDatumRow::~ObLoadDatumRow() {}

void ObLoadDatumRow::reset() {
  allocator_.reset();
  capacity_ = 0;
  count_ = 0;
  datums_ = nullptr;
}

int ObLoadDatumRow::init(int64_t capacity) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    reset();
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(datums_ = static_cast<ObStorageDatum *>(
                      allocator_.alloc(sizeof(ObStorageDatum) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (datums_) ObStorageDatum[capacity];
      capacity_ = capacity;
      count_ = capacity;
    }
  }
  return ret;
}

int64_t ObLoadDatumRow::get_deep_copy_size() const {
  int64_t size = 0;
  size += sizeof(ObStorageDatum) * count_;
  for (int64_t i = 0; i < count_; ++i) {
    size += datums_[i].get_deep_copy_size();
  }
  return size;
}

int ObLoadDatumRow::deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len,
                              int64_t &pos) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    datums = new (buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; ++i) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", KR(ret), K(src.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      value.id0 = src.value.id0;
      value.id1 = src.value.id1;
      capacity_ = datum_cnt;
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

DEF_TO_STRING(ObLoadDatumRow) {
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(capacity), K_(count));
  if (nullptr != datums_) {
    J_ARRAY_START();
    for (int64_t i = 0; i < count_; ++i) {
      databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
      pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
      databuff_printf(buf, buf_len, pos, ",");
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObLoadDatumRow) {
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(datums_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadDatumRow) {
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(count));
    } else if (count > capacity_ && OB_FAIL(init(count))) { // TODO: mcz
      LOG_WARN("fail to init", KR(ret));
    } else {
      OB_UNIS_DECODE_ARRAY(datums_, count);
      count_ = count;
      value.id0 = datums_[0].get_int();
      value.id1 = datums_[1].get_int32();
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadDatumRow) {
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, count_);
  return len;
}

/**
 * ObLoadDatumRowCompare
 */

ObLoadDatumRowCompare::ObLoadDatumRowCompare()
    : result_code_(OB_SUCCESS), rowkey_column_num_(0), datum_utils_(nullptr),
      is_inited_(false) {}

ObLoadDatumRowCompare::~ObLoadDatumRowCompare() {}

int ObLoadDatumRowCompare::init(int64_t rowkey_column_num,
                                const ObStorageDatumUtils *datum_utils) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDatumRowCompare init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_column_num <= 0 || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_column_num), KP(datum_utils));
  } else {
    rowkey_column_num_ = rowkey_column_num;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }
  return ret;
}

bool ObLoadDatumRowCompare::operator()(const ObLoadDatumRow *lhs,
                                       const ObLoadDatumRow *rhs) {
  return lhs->value < rhs->value;
}

/**
 * ObLoadRowCaster
 */

ObLoadRowCaster::ObLoadRowCaster()
    : column_count_(0), collation_type_(CS_TYPE_INVALID),
      cast_allocator_(ObModIds::OB_SQL_LOAD_DATA), is_inited_(false) {}

ObLoadRowCaster::~ObLoadRowCaster() {}

int ObLoadRowCaster::init(
    const ObTableSchema *table_schema,
    const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadRowCaster init twice", KR(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema ||
                         field_or_var_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(field_or_var_list));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(MTL_ID(),
                                            tz_info_.get_tz_map_wrap()))) {
    LOG_WARN("fail to get tenant time zone", KR(ret));
  } else if (OB_FAIL(init_column_schemas_and_idxs(table_schema,
                                                  field_or_var_list))) {
    LOG_WARN("fail to init column schemas and idxs", KR(ret));
  } else if (OB_FAIL(datum_row_.init(table_schema->get_column_count()))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    column_count_ = table_schema->get_column_count();
    collation_type_ = table_schema->get_collation_type();
    cast_allocator_.set_tenant_id(MTL_ID());
    is_inited_ = true;
  }
  return ret;
}

int ObLoadRowCaster::init_column_schemas_and_idxs(
    const ObTableSchema *table_schema,
    const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list) {
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    bool found_column = true;
    for (int64_t i = 0;
         OB_SUCC(ret) && OB_LIKELY(found_column) && i < column_descs.count();
         ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema =
          table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (OB_UNLIKELY(col_schema->is_hidden())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected hidden column", KR(ret), K(i), KPC(col_schema));
      } else if (OB_FAIL(column_schemas_.push_back(col_schema))) {
        LOG_WARN("fail to push back column schema", KR(ret));
      } else {
        found_column = false;
      }
      // find column in source data columns
      for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) &&
                          j < field_or_var_list.count();
           ++j) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct =
            field_or_var_list.at(j);
        if (col_desc.col_id_ == field_or_var_struct.column_id_) {
          found_column = true;
          if (OB_FAIL(column_idxs_.push_back(j))) {
            LOG_WARN("fail to push back column idx", KR(ret), K(column_idxs_),
                     K(i), K(col_desc), K(j), K(field_or_var_struct));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_column)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported incomplete column data", KR(ret), K(column_idxs_),
               K(column_descs), K(field_or_var_list));
    }
  }
  return ret;
}

int ObLoadRowCaster::get_casted_row(const ObNewRow &new_row,
                                    const ObLoadDatumRow *&datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadRowCaster not init", KR(ret));
  } else {
    const int64_t extra_col_cnt =
        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    cast_allocator_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_idxs_.count(); ++i) {
      int64_t column_idx = column_idxs_.at(i);
      if (OB_UNLIKELY(column_idx < 0 || column_idx >= new_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column idx", KR(ret), K(column_idx),
                 K(new_row.count_));
      } else {
        const ObColumnSchemaV2 *column_schema = column_schemas_.at(i);
        const ObObj &src_obj = new_row.cells_[column_idx];
        ObStorageDatum &dest_datum = datum_row_.datums_[i];
        if (OB_FAIL(cast_obj_to_datum(column_schema, src_obj, dest_datum))) {
          LOG_WARN("fail to cast obj to datum", KR(ret), K(src_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      datum_row_.value.id0 = datum_row_.datums_[0].get_int();
      datum_row_.value.id1 = datum_row_.datums_[1].get_int32();
      datum_row = &datum_row_;
    }
  }
  return ret;
}

int ObLoadRowCaster::cast_obj_to_datum(const ObColumnSchemaV2 *column_schema,
                                       const ObObj &obj,
                                       ObStorageDatum &datum) {
  int ret = OB_SUCCESS;
  ObDataTypeCastParams cast_params(&tz_info_);
  ObCastCtx cast_ctx(&cast_allocator_, &cast_params, CM_NONE, collation_type_);
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  ObObj casted_obj;
  if (obj.is_null()) {
    casted_obj.set_null();
  } else if (is_oracle_mode() &&
             (obj.is_null_oracle() || 0 == obj.get_val_len())) {
    casted_obj.set_null();
  } else if (is_mysql_mode() && 0 == obj.get_val_len() &&
             !ob_is_string_tc(expect_type)) {
    ObObj zero_obj;
    zero_obj.set_int(0);
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, zero_obj,
                                     casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(zero_obj), K(expect_type));
    }
  } else {
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(obj), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum.from_obj_enhance(casted_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(casted_obj));
    }
  }
  return ret;
}

/**
 * ObLoadDispatcher
 */

ObLoadDispatcher::ObLoadDispatcher(int thread_num, int dispatch_num)
    : is_inited_(false), thread_num_(thread_num), is_finished_(false),
      finish_smapling_(false), finish_smapling_stat_(false),
      allocator_(ObModIds::OB_SQL_LOAD_DATA), current_num_(0),
      dispatch_num_(dispatch_num) {}

ObLoadDispatcher::~ObLoadDispatcher() {
  for (int i = 0; i < dispatch_num_; i++) {
    dispatch_queue_[i]->reset();
  }
}

int ObLoadDispatcher::init(const ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (IS_INIT) {
    ret = OB_SUCCESS;
    LOG_INFO("ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (dispatch_num_ < 3 || dispatch_num_ > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    ObArray<ObColDesc> multi_version_column_descs;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(
            multi_version_column_descs))) {
      LOG_WARN("fail to get multi version column descs", KR(ret));
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs,
                                         rowkey_column_num, is_oracle_mode(),
                                         allocator_))) {
      LOG_WARN("fail to init datum utils", KR(ret));
    } else if (OB_FAIL(compare_.init(rowkey_column_num, &datum_utils_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(is_closed_.reserve(thread_num_))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(smapling_data_.reserve(SAMPLING_NUM))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(dispatch_queue_.reserve(dispatch_num_))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(dispatch_point_.reserve(dispatch_num_ - 1))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      is_inited_ = true;
      for (int i = 0; i < thread_num_; i++) {
        is_closed_.push_back(false);
      }
      for (int i = 0; i < dispatch_num_ && OB_SUCC(ret); i++) {
        void *buf = nullptr;
        ObSpinLock *dispatch_lock = nullptr;
        ObLoadDispatchQueue *dispatch_queue = nullptr;
        ObArenaAllocator *allocator1 = nullptr;
        ObArenaAllocator *allocator2 = nullptr;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSpinLock)))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(dispatch_lock = new (buf) ObSpinLock())) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(
                       buf = allocator_.alloc(sizeof(ObLoadDispatchQueue)))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(dispatch_queue = new (buf)
                                 ObLoadDispatchQueue(MEM_BUFFER_SIZE,SLEEP_TIME))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(buf =
                                 allocator_.alloc(sizeof(ObArenaAllocator)))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(allocator1 = new (buf) ObArenaAllocator(
                                 common::ObNewModIds::OB_SQL_LOAD_DATA,
                                 common::OB_MALLOC_BIG_BLOCK_SIZE, MTL_ID()))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(buf =
                                 allocator_.alloc(sizeof(ObArenaAllocator)))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (OB_ISNULL(allocator2 = new (buf) ObArenaAllocator(
                                 common::ObNewModIds::OB_SQL_LOAD_DATA,
                                 common::OB_MALLOC_BIG_BLOCK_SIZE, MTL_ID()))) {
          LOG_WARN("fail to alloc memory", KR(ret));
        } else {
          dispatch_queue->init(allocator1, allocator2);
          dispatch_queue_.push_back(dispatch_queue);
        }
      }
    }
  }
  return ret;
}

int ObLoadDispatcher::do_dispatch_all() {
  int ret = OB_SUCCESS;
  size_t smapling_size = smapling_data_.size();
  for (size_t i = 0; i < smapling_size; ++i) {
    ObLoadDatumRow *item = smapling_data_.at(i);
    if (OB_FAIL(do_dispatch_one(item))) {
      LOG_WARN("fail to do dispatch one", KR(ret));
      break;
    } else {
      ob_free((void *)item);
    }
  }
  smapling_data_.reset();
  return ret;
}

int ObLoadDispatcher::do_dispatch_one(const ObLoadDatumRow *item) {
  int ret = OB_SUCCESS;
  size_t dispatch_size = dispatch_point_.size();
  int idx = 0;

  // 二分查询桶
  size_t left = 0, right = dispatch_size;
  while (left <= right) {
    size_t mid = (left + right) / 2;
    if (mid > 0 && mid < dispatch_size) {
      if (!compare_(item, dispatch_point_[mid - 1]) &&
          compare_(item, dispatch_point_[mid])) {
        idx = mid;
        break;
      } else if (compare_(item, dispatch_point_[mid - 1])) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    } else if (mid == 0) {
      if (compare_(item, dispatch_point_[0])) {
        idx = 0;
        break;
      } else {
        left = 1;
      }
    } else {
      if (!compare_(item, dispatch_point_[dispatch_size - 1])) {
        idx = dispatch_size;
        break;
      } else {
        right = dispatch_size - 1;
      }
    }
  }

  // 查询桶
  // item < dispatch_point_[0]
  // if (compare_(item, dispatch_point_[0])) {
  //   idx = 0;
  // }
  // // item >= dispatch_point_[dispatch_size - 1]
  // else if (!compare_(item, dispatch_point_[dispatch_size - 1])) {
  //   idx = dispatch_size;
  // }
  // // item >= dispatch_point_[j - 1] and item < dispatch_point_[j]
  // else {
  //   for (size_t j = 1; j < dispatch_size; j++) {
  //     if (!compare_(item, dispatch_point_[j - 1]) &&
  //         compare_(item, dispatch_point_[j])) {
  //       idx = j;
  //       break;
  //     }
  //   }
  // }

  if (OB_FAIL(dispatch_queue_[idx]->push_item(item))) {
    LOG_WARN("fail to push back to dispatch queue", K(ret));
  }

  return ret;
}

int ObLoadDispatcher::do_stat() {
  int ret = OB_SUCCESS;
  std::sort(smapling_data_.begin(), smapling_data_.end(), compare_);
  if (OB_FAIL(compare_.result_code_)) {
    LOG_WARN("fail to do sort", KR(ret));
  } else {
    int split_point_num = dispatch_num_ - 1;
    int64_t stat_num =
        current_num_ < SAMPLING_NUM ? current_num_.load() : SAMPLING_NUM;
    for (int i = 0; i < split_point_num && OB_SUCC(ret); i++) {
      int index = (stat_num - 1) / (split_point_num - 1) * i;
      const ObLoadDatumRow &datum_row = *smapling_data_.at(index);
      char *buf = NULL;
      ObLoadDatumRow *new_item = NULL;
      const int64_t item_size =
          sizeof(ObLoadDatumRow) + datum_row.get_deep_copy_size();

      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(item_size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(item_size));
      } else if (OB_ISNULL(new_item = new (buf) ObLoadDatumRow())) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to placement new item", K(ret));
      } else {
        int64_t buf_pos = sizeof(ObLoadDatumRow);
        if (OB_FAIL(new_item->deep_copy(datum_row, buf, item_size, buf_pos))) {
          LOG_WARN("fail to deep copy item", K(ret));
        } else {
          dispatch_point_.push_back(new_item);
          LOG_INFO("new dispatch point", KP(new_item));
        }
      }
    }
  }
  return ret;
}

int ObLoadDispatcher::append_row(const ObLoadDatumRow &datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDispatcher not init", KR(ret), KP(this));
  } else if (is_finished_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed dispatcher", KR(ret));
  }

  bool just_finished_ = false;
  if (finish_smapling_) {
    current_num_++;
  } else {
    LockGuard guard(lock_);
    current_num_++;
    if (!finish_smapling_ && current_num_ == SAMPLING_NUM) {
      just_finished_ = true;
      finish_smapling_ = true;
    }
  }

  while (current_num_ > BUFFER_NUM)
    usleep(SLEEP_TIME);

  // 采样完成后，数据直接分发对应的桶内
  if (finish_smapling_ && !just_finished_) {
    while (!finish_smapling_stat_)
      usleep(SLEEP_TIME);
    if (OB_FAIL(do_dispatch_one(&datum_row))) {
      LOG_WARN("fail to do dispatch one", KR(ret));
    }
  }
  // 采样未完成，数据放到对应采样数组内，采样结束后再分发到对应的桶内
  else {
    char *buf = nullptr;
    ObLoadDatumRow *new_item = nullptr;
    const int64_t item_size =
        sizeof(ObLoadDatumRow) + datum_row.get_deep_copy_size();
    if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(
                      item_size, ObMemAttr(MTL_ID(), "dispatcher"))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(item_size));
    } else if (OB_ISNULL(new_item = new (buf) ObLoadDatumRow())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to placement new item", K(ret));
    } else {
      int64_t buf_pos = sizeof(ObLoadDatumRow);
      if (OB_FAIL(new_item->deep_copy(datum_row, buf, item_size, buf_pos))) {
        LOG_WARN("fail to deep copy item", K(ret));
      } else {
        {
          LockGuard guard(lock_);
          if (OB_FAIL(smapling_data_.push_back(new_item))) {
            LOG_WARN("fail to do push back", KR(ret));
          }
        }

        if (OB_SUCC(ret) && just_finished_) {
          if (OB_FAIL(do_stat())) {
            LOG_WARN("fail to do stat", KR(ret));
          } else if (OB_FAIL(do_dispatch_all())) {
            LOG_WARN("fail to do dispatch all", KR(ret));
          } else {
            finish_smapling_stat_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObLoadDispatcher::get_next_row(int idx, const ObLoadDatumRow *&datum_row) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx >= dispatch_num_ || idx < 0)) {
    return OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    while (!finish_smapling_stat_)
      usleep(SLEEP_TIME);
    if (OB_FAIL(dispatch_queue_[idx]->pop_item(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      }
    } else {
      current_num_--;
    }
  }
  return ret;
}

int ObLoadDispatcher::close(int idx) {
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDispatcher not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_[idx])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed ObLoadDispatcher", KR(ret));
  } else {
    is_closed_[idx] = true;
    bool is_finish = true;
    for (size_t i = 0; i < is_closed_.size(); i++) {
      if (!is_closed_[i]) {
        is_finish = false;
        break;
      }
    }
    if (is_finish) {
      is_finished_ = true;
      if (!finish_smapling_) {
        if (OB_FAIL(do_stat())) {
          LOG_WARN("fail to do stat", KR(ret));
        } else if (OB_FAIL(do_dispatch_all())) {
          LOG_WARN("fail to do dispatch all", KR(ret));
        } else {
          finish_smapling_ = true;
          finish_smapling_stat_ = true;
        }
      }
      for (int i = 0; i < dispatch_num_; i++) {
        dispatch_queue_[i]->finish();
      }
    }
  }
  return ret;
}

DispatchQueue<ObLoadDatumRow> *ObLoadDispatcher::get_dispatch_queue(int idx) {
  if (OB_UNLIKELY(idx >= dispatch_num_ || idx < 0)) {
    LOG_WARN("invalid args");
    return nullptr;
  } else {
    return dispatch_queue_[idx];
  }
}

void ObLoadDispatcher::debug_print() {
  int ret = OB_SUCCESS;
  auto print_func = [&]() {
    while (!this->is_finished_) {
      usleep(SLEEP_TIME);
      LOG_INFO("[OB_LOAD_INFO]", "current_num", this->current_num_.load());
      for (int i = 0; i < this->dispatch_num_; i++) {
        int64_t pop_size=this->dispatch_queue_[i]->get_pop_size()/(1LL<<20);
        int64_t push_size = this->dispatch_queue_[i]->get_push_size()/(1LL << 20);
        int64_t remain_size = push_size - pop_size;
        LOG_INFO("[OB_LOAD_INFO]", "dispatch", i, "pop_size",pop_size, "push_size",push_size, "remain_size",remain_size);
      }
    }
  };
  std::thread debug_thread(print_func);
  debug_thread.detach();
}

/**
 * ObLoadExternalSort
 */

ObLoadExternalSort::ObLoadExternalSort()
    : allocator_(ObModIds::OB_SQL_LOAD_DATA), is_closed_(false),
      is_inited_(false) {}

ObLoadExternalSort::~ObLoadExternalSort() { external_sort_.clean_up(); }

int ObLoadExternalSort::init(const ObTableSchema *table_schema,
                             int64_t mem_size, int64_t file_buf_size) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadExternalSort init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    ObArray<ObColDesc> multi_version_column_descs;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(
            multi_version_column_descs))) {
      LOG_WARN("fail to get multi version column descs", KR(ret));
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs,
                                         rowkey_column_num, is_oracle_mode(),
                                         allocator_))) {
      LOG_WARN("fail to init datum utils", KR(ret));
    } else if (OB_FAIL(compare_.init(rowkey_column_num, &datum_utils_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(external_sort_.init(mem_size, file_buf_size, 0, MTL_ID(),
                                           &compare_))) {
      LOG_WARN("fail to init external sort", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadExternalSort::append_row(const ObLoadDatumRow &datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.add_item(datum_row))) {
    LOG_WARN("fail to add item", KR(ret));
  }
  return ret;
}

int ObLoadExternalSort::append_row_reuse(const ObLoadDatumRow &datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.add_item_reuse(datum_row))) {
    LOG_WARN("fail to add item", KR(ret));
  }
  return ret;
}

int ObLoadExternalSort::close() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.do_sort_reuse(true))) {
    LOG_WARN("fail to do sort", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

int ObLoadExternalSort::get_next_row(const ObLoadDatumRow *&datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.get_next_item(datum_row))) {
    LOG_WARN("fail to get next item", KR(ret));
  }
  return ret;
}

/**
 * ObLoadSSTableWriter
 */

ObLoadSSTableWriter::ObLoadSSTableWriter()
    : rowkey_column_num_(0), extra_rowkey_column_num_(0), column_count_(0),
      is_inited_(false), is_finished_(false) {}

ObLoadSSTableWriter::~ObLoadSSTableWriter() {}

int ObLoadSSTableWriter::init(const ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  lock_.lock();
  if (IS_INIT) {
    ret = OB_SUCCESS;
    LOG_INFO("ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    tablet_id_ = table_schema->get_tablet_id();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ =
        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = table_schema->get_column_count();
    ObLocationService *location_service = nullptr;
    bool is_cache_hit = false;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location service is null", KR(ret), KP(location_service));
    } else if (OB_FAIL(location_service->get(MTL_ID(), tablet_id_, INT64_MAX,
                                             is_cache_hit, ls_id_))) {
      LOG_WARN("fail to get ls id", KR(ret), K(tablet_id_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle_,
                                          ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle_))) {
      LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(init_sstable_index_builder(table_schema))) {
      LOG_WARN("fail to init sstable index builder", KR(ret));
    } else if (OB_FAIL(init_data_store_desc(table_schema))) {
      LOG_WARN("fail to init data store desc", KR(ret));
    } else {
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key_.tablet_id_ = tablet_id_;
      table_key_.log_ts_range_.start_log_ts_ = 0;
      table_key_.log_ts_range_.end_log_ts_ = ObTimeUtil::current_time_ns();
      is_inited_ = true;
    }
  }
  lock_.unlock();
  return ret;
}

int ObLoadSSTableWriter::init_sstable_index_builder(
    const ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_FAIL(
          data_desc.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1L))) {
    LOG_WARN("fail to init data desc", KR(ret));
  } else {
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.need_prebuild_bloomfilter_ = false;
    data_desc.col_desc_array_.reset();
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      LOG_WARN("fail to reserve column desc array", KR(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(
                   data_desc.col_desc_array_))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(
                   data_desc.col_desc_array_))) {
      LOG_WARN("fail to add extra rowkey cols", KR(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ +
                                          OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        LOG_WARN("fail to push back last col for index", KR(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_index_builder_.init(data_desc))) {
      LOG_WARN("fail to init index builder", KR(ret), K(data_desc));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_data_store_desc(
    const share::schema::ObTableSchema *table_schema) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_store_desc_.init(*table_schema, ls_id_, tablet_id_,
                                    MAJOR_MERGE, 1))) {
    LOG_WARN("fail to init data_store_desc", KR(ret), K(tablet_id_));
  } else {
    data_store_desc_.sstable_index_builder_ = &sstable_index_builder_;
  }
  return ret;
}
int ObLoadSSTableWriter::init_macro_block_writer(const int64_t parallel_idx) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_FAIL(
                 datum_row_.init(column_count_ + extra_rowkey_column_num_))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    ObMacroDataSeq data_seq;
    data_seq.set_parallel_degree(parallel_idx);
    if (OB_FAIL(macro_block_writer_.open(data_store_desc_, data_seq))) {
      LOG_WARN("fail to init macro block writer", KR(ret), K(data_store_desc_),
               K(data_seq));
    } else {
      is_closed_ = false;
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      datum_row_.storage_datums_[rowkey_column_num_].set_int(
          -1); // fill trans_version
      datum_row_.storage_datums_[rowkey_column_num_ + 1].set_int(
          0); // fill sql_no
    }
  }
  return ret;
}

int ObLoadSSTableWriter::append_row(const ObLoadDatumRow &datum_row) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() ||
                         datum_row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(column_count_));
  } else {
    for (int64_t i = 0; i < column_count_; ++i) {
      if (i < rowkey_column_num_) {
        datum_row_.storage_datums_[i] = datum_row.datums_[i];
      } else {
        datum_row_.storage_datums_[i + extra_rowkey_column_num_] =
            datum_row.datums_[i];
      }
    }
    if (OB_FAIL(macro_block_writer_.append_row(datum_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::create_sstable() {
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  SMART_VAR(ObSSTableMergeRes, merge_res) {
    const ObStorageSchema &storage_schema =
        tablet_handle_.get_obj()->get_storage_schema();
    int64_t column_count = 0;
    if (OB_FAIL(
            storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", KR(ret));
    } else if (OB_FAIL(sstable_index_builder_.close(column_count, merge_res))) {
      LOG_WARN("fail to close sstable index builder", KR(ret));
    } else {
      ObTabletCreateSSTableParam create_param;
      create_param.table_key_ = table_key_;
      create_param.table_mode_ = storage_schema.get_table_mode_struct();
      create_param.index_type_ = storage_schema.get_index_type();
      create_param.rowkey_column_cnt_ =
          storage_schema.get_rowkey_column_num() +
          ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      create_param.schema_version_ = storage_schema.get_schema_version();
      create_param.create_snapshot_version_ = 0;
      ObSSTableMergeRes::fill_addr_and_data(merge_res.root_desc_,
                                            create_param.root_block_addr_,
                                            create_param.root_block_data_);
      ObSSTableMergeRes::fill_addr_and_data(
          merge_res.data_root_desc_, create_param.data_block_macro_meta_addr_,
          create_param.data_block_macro_meta_);
      create_param.root_row_store_type_ = merge_res.root_desc_.row_type_;
      create_param.data_index_tree_height_ = merge_res.root_desc_.height_;
      create_param.index_blocks_cnt_ = merge_res.index_blocks_cnt_;
      create_param.data_blocks_cnt_ = merge_res.data_blocks_cnt_;
      create_param.micro_block_cnt_ = merge_res.micro_block_cnt_;
      create_param.use_old_macro_block_count_ =
          merge_res.use_old_macro_block_count_;
      create_param.row_count_ = merge_res.row_count_;
      create_param.column_cnt_ = merge_res.data_column_cnt_;
      create_param.data_checksum_ = merge_res.data_checksum_;
      create_param.occupy_size_ = merge_res.occupy_size_;
      create_param.original_size_ = merge_res.original_size_;
      create_param.max_merged_trans_version_ =
          merge_res.max_merged_trans_version_;
      create_param.contain_uncommitted_row_ =
          merge_res.contain_uncommitted_row_;
      create_param.compressor_type_ = merge_res.compressor_type_;
      create_param.encrypt_id_ = merge_res.encrypt_id_;
      create_param.master_key_id_ = merge_res.master_key_id_;
      create_param.data_block_ids_ = merge_res.data_block_ids_;
      create_param.other_block_ids_ = merge_res.other_block_ids_;
      MEMCPY(create_param.encrypt_key_, merge_res.encrypt_key_,
             OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      if (OB_FAIL(merge_res.fill_column_checksum(
              &storage_schema, create_param.column_checksums_))) {
        LOG_WARN("fail to fill column checksum for empty major", KR(ret),
                 K(create_param));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(
                     create_param, table_handle))) {
        LOG_WARN("fail to create sstable", KR(ret), K(create_param));
      } else {
        const int64_t rebuild_seq = ls_handle_.get_ls()->get_rebuild_seq();
        ObTabletHandle new_tablet_handle;
        ObUpdateTableStoreParam table_store_param(
            table_handle, tablet_handle_.get_obj()->get_snapshot_version(),
            false, &storage_schema, rebuild_seq, true, true);
        if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(
                tablet_id_, table_store_param, new_tablet_handle))) {
          LOG_WARN("fail to update tablet table store", KR(ret), K(tablet_id_),
                   K(table_store_param));
        }
      }
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed sstable writer", KR(ret));
  } else {
    if (OB_FAIL(macro_block_writer_.close())) {
      LOG_WARN("fail to close macro block writer", KR(ret));
    } else {
      // 重置保证下次能够重用
      datum_row_.reset();
      macro_block_writer_.reset();
      is_closed_ = true;
    }
  }
  return ret;
}

int ObLoadSSTableWriter::finish() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_finished_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected finished sstable writer", KR(ret));
  } else {
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(create_sstable())) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else {
      is_finished_ = true;
    }
  }
  return ret;
}

/**
 * ObLoadDataDirectDemo
 */

ObLoadDataDirectDemo::ObLoadDataDirectDemo() {}

ObLoadDataDirectDemo::~ObLoadDataDirectDemo() {}

int ObLoadDataDirectDemo::execute(ObExecContext &ctx,
                                  ObLoadDataStmt &load_stmt) {
  int ret = OB_SUCCESS;
  if (stage_process_) {
    if (OB_FAIL(do_process())) {
      LOG_WARN("fail to do load", KR(ret));
    }
  } else {
    if (OB_FAIL(do_load())) {
      LOG_WARN("fail to do process", KR(ret));
    }
  }
  return ret;
}

int ObLoadDataDirectDemo::init(int64_t index, ObLoadDataStmt &load_stmt,
                               int64_t offset, int64_t end, bool stage_process,
                               ObLoadDispatcher *dispatcher,
                               ObLoadSSTableWriter *sstable_writer) {
  int ret = OB_SUCCESS;
  index_ = index;
  stage_process_ = stage_process;
  dispatcher_ = dispatcher;
  sstable_writer_ = sstable_writer;
  if (stage_process_) {
    if (OB_FAIL(inner_init_process(load_stmt))) {
      LOG_WARN("fail to init ObLoadDataDirectDemo process", KR(ret));
    } else if (OB_FAIL(file_reader_.set_offset_end(offset, end))) {
      LOG_WARN("fail to set offset and end", KR(ret));
    }
  } else {
    if (OB_FAIL(inner_init_load(load_stmt))) {
      LOG_WARN("fail to init ObLoadDataDirectDemo load", KR(ret));
    }
  }
  return ret;
}

int ObLoadDataDirectDemo::inner_init_process(ObLoadDataStmt &load_stmt) {
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
      load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(
          ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id,
                                                   table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support heap table", KR(ret));
  }
  // init csv_parser_
  else if (OB_FAIL(csv_parser_.init(load_stmt.get_data_struct_in_file(),
                                    field_or_var_list.count(),
                                    load_args.file_cs_type_))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  }
  // init file_reader_
  else if (OB_FAIL(file_reader_.open(load_args.full_file_path_))) {
    LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
  }
  // init buffer_
  else if (OB_FAIL(buffer_.create(FILE_BUFFER_SIZE))) {
    LOG_WARN("fail to create buffer", KR(ret));
  }
  // init row_caster_
  else if (OB_FAIL(row_caster_.init(table_schema, field_or_var_list))) {
    LOG_WARN("fail to init row caster", KR(ret));
  }
  // init dispatcher_
  else if (OB_FAIL(dispatcher_->init(table_schema))) {
    LOG_WARN("fail to init dispatcher", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectDemo::inner_init_load(ObLoadDataStmt &load_stmt) {
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
      load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(
          ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id,
                                                   table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support heap table", KR(ret));
  }

  // init dispatcher_
  if (OB_FAIL(dispatcher_->init(table_schema))) {
    LOG_WARN("fail to init dispatcher", KR(ret));
  }
  // init external_sort_
  else if (OB_FAIL(external_sort_.init(table_schema, MEM_BUFFER_SIZE,
                                       FILE_BUFFER_SIZE))) {
    LOG_WARN("fail to init external sort", KR(ret));
  }
  // init sstable_writer_
  else if (OB_FAIL(sstable_writer_->init(table_schema))) {
    LOG_WARN("fail to init sstable writer", KR(ret));
  }
  // init macro_block_writer_
  else if (OB_FAIL(sstable_writer_->init_macro_block_writer(index_))) {
    LOG_WARN("failed to init macro block writer", K(ret));
  }
  // set memory sort allocator
  else {
    external_sort_.set_dispatch_queue(dispatcher_->get_dispatch_queue(index_));
  }
  return ret;
}

int ObLoadDataDirectDemo::do_process() {
  int ret = OB_SUCCESS;
  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(buffer_.squash())) {
      LOG_WARN("fail to squash buffer", KR(ret));
    } else if (OB_FAIL(file_reader_.read_next_buffer(buffer_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to read next buffer", KR(ret));
      } else {
        if (OB_UNLIKELY(!buffer_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected incomplate data", KR(ret));
        }
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(buffer_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty buffer", KR(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(csv_parser_.get_next_row(buffer_, new_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(row_caster_.get_casted_row(*new_row, datum_row))) {
          LOG_WARN("fail to cast row", KR(ret));
        } else if (OB_FAIL(dispatcher_->append_row(*datum_row))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dispatcher_->close(index_))) {
      LOG_WARN("fail to close dispatcher", KR(ret));
    }
  }
  return ret;
}

int ObLoadDataDirectDemo::do_load() {
  int ret = OB_SUCCESS;
  const ObLoadDatumRow *datum_row = nullptr;

  int count_ = 0;
  clock_t start, end;
  int64_t t;

  start = clock();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(dispatcher_->get_next_row(index_, datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(external_sort_.append_row_reuse(*datum_row))) {
      LOG_WARN("fail to append row", KR(ret));
    } else {
      count_++;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(external_sort_.close())) {
      LOG_WARN("fail to close external sort", KR(ret));
    }
  }

  end=clock();
  t=(end-start)/CLOCKS_PER_SEC;
  LOG_WARN("[OB_LOAD_INFO]", "thread", index_, "external sort time",t);

  while (OB_SUCC(ret)) {
    if (OB_FAIL(external_sort_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(sstable_writer_->append_row(*datum_row))) {
      LOG_WARN("fail to append row", KR(ret), K(*datum_row));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_writer_->close())) {
      LOG_WARN("fail to close sstable writer", KR(ret));
    }
  }

  end = clock();
  t = (end - start) / CLOCKS_PER_SEC;
  LOG_WARN("[OB_LOAD_INFO]", "thread", index_, "sstable time",t);

  LOG_WARN("[OB_LOAD_INFO]", "thread", index_, "load data num", count_);

  return ret;
}

} // namespace sql
} // namespace oceanbase
