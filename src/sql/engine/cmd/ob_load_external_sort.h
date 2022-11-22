#ifndef OCEANBASE_STORAGE_MY_PARALLEL_EXTERNAL_SORT_H_
#define OCEANBASE_STORAGE_MY_PARALLEL_EXTERNAL_SORT_H_

#include "lib/ob_define.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_vector.h"
#include "share/io/ob_io_manager.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "share/config/ob_server_config.h"
#include "storage/ob_parallel_external_sort.h"
#include <cstdlib>

namespace oceanbase
{
namespace storage
{




template<typename T, typename Compare>
class MyExternalSortRound
{
public:
  MyExternalSortRound();
  virtual ~MyExternalSortRound();
  int init(const int64_t merge_count, const int64_t file_buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, Compare *compare);
  bool is_inited() const { return is_inited_; }
  int add_item(const T &item);
  int build_fragment();
  int do_merge(MyExternalSortRound &next_round);
  int do_one_run(const int64_t start_reader_idx, MyExternalSortRound &next_round);
  int finish_write();
  int clean_up();
  int build_merger();
  int get_next_item(const T *&item);
  int64_t get_fragment_count();
  int add_fragment_iter(ObFragmentIterator<T> *iter);
  int transfer_final_sorted_fragment_iter(MyExternalSortRound &dest_round);
private:
  typedef ObFragmentReaderV2<T> FragmentReader;
  typedef ObFragmentIterator<T> FragmentIterator;
  typedef common::ObArray<FragmentIterator *> FragmentIteratorList;
  typedef ObFragmentWriterV2<T> FragmentWriter;
  typedef ObFragmentMerge<T, Compare> FragmentMerger;
  bool is_inited_;
  int64_t merge_count_;
  int64_t file_buf_size_;
  static FragmentIteratorList iters_;
  static common::ObSpinLock iters_lock_;
  FragmentWriter writer_;
  int64_t expire_timestamp_;
  Compare *compare_;
public:
  static FragmentMerger merger_;
private:
  common::ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  bool is_writer_opened_;
};

template<typename T, typename Compare>
ObFragmentMerge<T, Compare> MyExternalSortRound<T,Compare>::merger_;


template<typename T, typename Compare>
common::ObArray<ObFragmentIterator<T> *> MyExternalSortRound<T,Compare>::iters_;
template<typename T, typename Compare>
common::ObSpinLock MyExternalSortRound<T,Compare>::iters_lock_;

template<typename T, typename Compare>
MyExternalSortRound<T, Compare>::MyExternalSortRound()
  : is_inited_(false), merge_count_(0), file_buf_size_(0), writer_(),
    expire_timestamp_(0), compare_(NULL), 
    //merger_(),
    allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE),
    tenant_id_(common::OB_INVALID_ID), dir_id_(-1), is_writer_opened_(false)
{
}

template<typename T, typename Compare>
MyExternalSortRound<T, Compare>::~MyExternalSortRound()
{
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::init(
    const int64_t merge_count, const int64_t file_buf_size, const int64_t expire_timestamp,
    const uint64_t tenant_id, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  //if (OB_UNLIKELY(is_inited_)) {
  //  ret = common::OB_INIT_TWICE;
  //  STORAGE_LOG(WARN, "MyExternalSortRound has been inited", K(ret));
  //} else 
  if (merge_count < ObExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT
      || file_buf_size % DIO_ALIGN_SIZE != 0
      || common::OB_INVALID_ID == tenant_id
      || NULL == compare) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(merge_count), K(file_buf_size),
        KP(compare));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
    STORAGE_LOG(WARN, "fail to alloc dir", K(ret));
  } else {
    is_inited_ = true;
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

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "MyExternalSortRound timeout", K(ret), K(expire_timestamp_));
  } else if (!is_writer_opened_ && OB_FAIL(writer_.open(file_buf_size_,
      expire_timestamp_, tenant_id_, dir_id_ + GETTID()) )) {
    STORAGE_LOG(WARN, "fail to open writer", K(ret), K_(tenant_id), K_(dir_id));
  } else {
    is_writer_opened_ = true;
    if (OB_FAIL(writer_.write_item(item))) {
      STORAGE_LOG(WARN, "fail to write item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::build_fragment()
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  FragmentReader *reader = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(FragmentReader)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(reader = new (buf) FragmentReader())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new FragmentReader", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to sync macro file", K(ret));
  } else {
    STORAGE_LOG(INFO, "build fragment", K(writer_.get_fd()), K(writer_.get_sample_item()));
    if (OB_FAIL(reader->init(writer_.get_fd(), writer_.get_dir_id(), expire_timestamp_, tenant_id_,
        writer_.get_sample_item(), file_buf_size_))) {
      STORAGE_LOG(WARN, "fail to open reader", K(ret), K(file_buf_size_),
          K(expire_timestamp_));
    } else {
      iters_lock_.lock();
      if(OB_FAIL(iters_.push_back(reader))) {
        iters_lock_.unlock();
        STORAGE_LOG(WARN, "fail to push back reader", K(ret));
      } else {
        iters_lock_.unlock();
        writer_.reset();
        is_writer_opened_ = false;
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::add_fragment_iter(ObFragmentIterator<T> *iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(iters_.push_back(iter))) {
    STORAGE_LOG(WARN, "fail to add iterator", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    MyExternalSortRound<T, Compare> &dest_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
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

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::build_merger()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(merger_.init(iters_, compare_))) {
    STORAGE_LOG(WARN, "fail to init FragmentMerger", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::finish_write()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else if (OB_FAIL(writer_.sync())) {
    STORAGE_LOG(WARN, "fail to finish writer", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::do_merge(
    MyExternalSortRound &next_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
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

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::do_one_run(
    const int64_t start_reader_idx, MyExternalSortRound &next_round)
{
  int ret = common::OB_SUCCESS;
  const T *item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t end_reader_idx = std::min(start_reader_idx + merge_count_, iters_.count());
    FragmentIteratorList iters;
    for (int64_t i = start_reader_idx; OB_SUCC(ret) && i < end_reader_idx; ++i) {
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

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
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

template<typename T, typename Compare>
int64_t MyExternalSortRound<T, Compare>::get_fragment_count()
{
  return iters_.count();
}

template<typename T, typename Compare>
int MyExternalSortRound<T, Compare>::clean_up()
{
  int ret = common::OB_SUCCESS;
  int tmp_ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSortRound has not been inited", K(ret));
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
  merge_count_ = 0;
  file_buf_size_ = 0;
  //iters_.reset();
  expire_timestamp_ = 0;
  compare_ = NULL;
  merger_.reset();
  allocator_.reset();
  return ret;
}


template<typename T, typename Compare>
class MyMemorySortRound
{
public:
  typedef MyExternalSortRound<T, Compare> ExternalSortRound;
  MyMemorySortRound();
  virtual ~MyMemorySortRound();
  int init(const int64_t mem_limit, const int64_t expire_timestamp,
      Compare *compare, ExternalSortRound *next_round);
  int add_item(const T &item);
  int build_fragment();
  virtual int get_next_item(const T *&item);
  int finish();
  bool is_in_memory() const { return is_in_memory_; }
  bool has_data() const { return has_data_; }
  void reset();
  int transfer_final_sorted_fragment_iter(ExternalSortRound &dest_round);
  TO_STRING_KV(K(is_inited_), K(is_in_memory_), K(has_data_), K(buf_mem_limit_),
      K(expire_timestamp_), KP(next_round_), KP(compare_), KP(iter_));
private:
  int build_iterator();
public:
  bool is_inited_;
private:
  bool is_in_memory_;
  bool has_data_;
  int64_t buf_mem_limit_;
  int64_t expire_timestamp_;
  ExternalSortRound *next_round_;
  common::ObArenaAllocator allocator_;
  common::ObVector<T *> item_list_;
  Compare *compare_;
  ObMemoryFragmentIterator<T> *iter_;
};

template<typename T, typename Compare>
MyMemorySortRound<T, Compare>::MyMemorySortRound()
  : is_inited_(false), is_in_memory_(false), has_data_(false), buf_mem_limit_(0), expire_timestamp_(0),
    next_round_(NULL), allocator_(common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER, common::OB_MALLOC_BIG_BLOCK_SIZE), item_list_(NULL, common::ObNewModIds::OB_ASYNC_EXTERNAL_SORTER),
    compare_(NULL), iter_(NULL)
{
}

template<typename T, typename Compare>
MyMemorySortRound<T, Compare>::~MyMemorySortRound()
{
}

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::init(
    const int64_t mem_limit, const int64_t expire_timestamp,
    Compare *compare, ExternalSortRound *next_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "MyMemorySortRound has been inited", K(ret));
  } else if (mem_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || NULL == compare
      || NULL == next_round
      || !next_round->is_inited()) {
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

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  const int64_t item_size = sizeof(T) + item.get_deep_copy_size();
  char *buf = NULL;
  T *new_item = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (ObExternalSortConstant::is_timeout(expire_timestamp_)) {
    ret = common::OB_TIMEOUT;
    STORAGE_LOG(WARN, "MyMemorySortRound timeout", K(ret), K(expire_timestamp_));
  } else if (item_size > buf_mem_limit_) {
    ret = common::OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "invalid item size, must not larger than buf memory limit",
        K(ret), K(item_size), K(buf_mem_limit_));
  } else if (allocator_.used() + item_size > buf_mem_limit_ && OB_FAIL(build_fragment())) {
    STORAGE_LOG(WARN, "fail to build fragment", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(item_size)))) {
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

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::build_fragment()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (item_list_.size() > 0) {
    int64_t start = common::ObTimeUtility::current_time();
    std::sort(item_list_.begin(), item_list_.end(), *compare_);
    if (OB_FAIL(compare_->result_code_)) {
      ret = compare_->result_code_;
    } else {
      const int64_t sort_fragment_time = common::ObTimeUtility::current_time() - start;
      STORAGE_LOG(INFO, "MyMemorySortRound", K(sort_fragment_time));
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
        const int64_t write_fragment_time = common::ObTimeUtility::current_time() - start;
        STORAGE_LOG(INFO, "MyMemorySortRound", K(write_fragment_time));
        item_list_.reset();
        allocator_.reuse();
      }
    }
  }
  return ret;
}

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::finish()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (0 == item_list_.size()) {
    has_data_ = false;
  //} else if (0 == next_round_->get_fragment_count()) {
  //  is_in_memory_ = true;
  //  has_data_ = true;
  //  std::sort(item_list_.begin(), item_list_.end(), *compare_);
  //  if (OB_FAIL(compare_->result_code_)) {
  //    STORAGE_LOG(WARN, "fail to sort item list", K(ret));
  //  }
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

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::build_iterator()
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMemoryFragmentIterator<T>)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for ObMemoryFragmentIterator", K(ret));
  } else if (OB_ISNULL(iter_ = new (buf) ObMemoryFragmentIterator<T>())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to placement new ObMemoryFragmentIterator", K(ret));
  } else if (OB_FAIL(iter_->init(item_list_))) {
    STORAGE_LOG(WARN, "fail to init iterator", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (NULL == iter_) {
    if (OB_FAIL(build_iterator())) {
      STORAGE_LOG(WARN, "fail to build iterator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == iter_) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, iter must not be null", K(ret), KP(iter_));
    } else if (OB_FAIL(iter_->get_next_item(item))) {
      STORAGE_LOG(WARN, "fail to get next item", K(ret));
    }
  }
  return ret;
}

template<typename T, typename Compare>
void MyMemorySortRound<T, Compare>::reset()
{
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

template<typename T, typename Compare>
int MyMemorySortRound<T, Compare>::transfer_final_sorted_fragment_iter(
    ExternalSortRound &dest_round)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyMemorySortRound has not been inited", K(ret));
  } else if (!is_in_memory()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "MyMemorySortRound has not data", K(ret));
  } else if (NULL == iter_ && OB_FAIL(build_iterator())) {
    STORAGE_LOG(WARN, "fail to build iterator", K(ret));
  } else if (OB_FAIL(dest_round.add_fragment_iter(iter_))) {
    STORAGE_LOG(WARN, "fail to add fragment iterator", K(ret));
  } else {
    iter_ = NULL;
  }
  return ret;
}




template<typename T, typename Compare>
class MyExternalSort
{
public:
  typedef MyMemorySortRound<T, Compare> MemorySortRound;
  typedef MyExternalSortRound<T, Compare> ExternalSortRound;
  MyExternalSort();
  virtual ~MyExternalSort();
  int init(const int64_t mem_limit, const int64_t file_buf_size, const int64_t expire_timestamp,
      const uint64_t tenant_id, Compare *compare);
  int add_item(const T &item);
  int do_sort(const bool final_merge);
  int get_next_item(const T *&item);
  void clean_up();
  int add_fragment_iter(ObFragmentIterator<T> *iter);
  int transfer_final_sorted_fragment_iter(MyExternalSort<T, Compare> &merge_sorter);
  int get_current_round(ExternalSortRound *&round);
  TO_STRING_KV(K(is_inited_), K(file_buf_size_), K(buf_mem_limit_), K(expire_timestamp_),
      K(merge_count_per_round_), KP(tenant_id_), KP(compare_));
private:
  static const int64_t EXTERNAL_SORT_ROUND_CNT = 2;
public:
  bool is_inited_;
private:
  static int64_t file_buf_size_;
  static int64_t buf_mem_limit_;
  static int64_t expire_timestamp_;
  static int64_t merge_count_per_round_;
  static Compare *compare_;
public:
  MemorySortRound memory_sort_round_;
private:
  static ExternalSortRound sort_rounds_[EXTERNAL_SORT_ROUND_CNT];
  static ExternalSortRound *curr_round_;
  static ExternalSortRound *next_round_;
public:  
  bool is_empty_;
private:
  uint64_t tenant_id_;
};

template<typename T, typename Compare>
int64_t MyExternalSort<T, Compare>::file_buf_size_;

template<typename T, typename Compare>
int64_t MyExternalSort<T, Compare>::buf_mem_limit_;

template<typename T, typename Compare>
int64_t MyExternalSort<T, Compare>::expire_timestamp_;

template<typename T, typename Compare>
int64_t MyExternalSort<T, Compare>::merge_count_per_round_;

template<typename T, typename Compare>
Compare* MyExternalSort<T, Compare>::compare_;




//template<typename T, typename Compare>
//MyExternalSortRound<T, Compare> MyExternalSort<T, Compare>::memory_sort_round_; 

template<typename T, typename Compare>
MyExternalSortRound<T, Compare> MyExternalSort<T, Compare>::sort_rounds_[EXTERNAL_SORT_ROUND_CNT];

template<typename T, typename Compare>
MyExternalSortRound<T, Compare>* MyExternalSort<T, Compare>::curr_round_;

template<typename T, typename Compare>
MyExternalSortRound<T, Compare>* MyExternalSort<T, Compare>::next_round_;

template<typename T, typename Compare>
MyExternalSort<T, Compare>::MyExternalSort()
  : is_inited_(false), 
    //file_buf_size_(0), buf_mem_limit_(0), expire_timestamp_(0), merge_count_per_round_(0),
    //compare_(NULL), 
    memory_sort_round_(), 
    //curr_round_(NULL), next_round_(NULL),
    is_empty_(true), tenant_id_(common::OB_INVALID_ID)
{
}

template<typename T, typename Compare>
MyExternalSort<T, Compare>::~MyExternalSort()
{
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::init(
    const int64_t mem_limit, const int64_t file_buf_size, const int64_t expire_timestamp,
    const uint64_t tenant_id, Compare *compare)
{
  int ret = common::OB_SUCCESS;
  int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "MyExternalSort has already been inited", K(ret));
  } else if (mem_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || file_buf_size % DIO_ALIGN_SIZE != 0
      || file_buf_size < macro_block_size
      || common::OB_INVALID_ID == tenant_id
      || NULL == compare) {
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
    if (merge_count_per_round_ < ObExternalSortConstant::MIN_MULTIPLE_MERGE_COUNT) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument, invalid memory limit", K(ret),
          K(buf_mem_limit_), K(file_buf_size_), K(merge_count_per_round_));
    } else if (OB_FAIL(curr_round_->init(merge_count_per_round_, file_buf_size_,
        expire_timestamp, tenant_id_, compare_))) {
      STORAGE_LOG(WARN, "fail to init current sort round", K(ret));
    } else if (OB_FAIL(memory_sort_round_.init(buf_mem_limit_,
        expire_timestamp, compare_, curr_round_))) {
      STORAGE_LOG(WARN, "fail to init memory sort round", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::add_item(const T &item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.add_item(item))) {
    STORAGE_LOG(WARN, "fail to add item in memory sort round", K(ret));
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::do_sort(const bool final_merge)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(memory_sort_round_.finish())) {
    STORAGE_LOG(WARN, "fail to finish memory sort round", K(ret));
  } else if (memory_sort_round_.has_data() && memory_sort_round_.is_in_memory()) {
    STORAGE_LOG(INFO, "all data sorted in memory");
    is_empty_ = false;
  } else if (0 == curr_round_->get_fragment_count()) {
    is_empty_ = true;
    ret = common::OB_SUCCESS;
  } else {
    // final_merge = true is for performance optimization, the count of fragments is reduced to lower than merge_count_per_round,
    // then the last round of merge this fragment is skipped
    const int64_t final_round_limit = final_merge ? merge_count_per_round_ : 1;
    int64_t round_id = 1;
    is_empty_ = false;
    while (OB_SUCC(ret) && curr_round_->get_fragment_count() > final_round_limit) {
      const int64_t start_time = common::ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "do sort start round", K(round_id));
      if (OB_FAIL(next_round_->init(merge_count_per_round_, file_buf_size_,
          expire_timestamp_, tenant_id_, compare_))) {
        STORAGE_LOG(WARN, "fail to init next sort round", K(ret));
      } else if (OB_FAIL(curr_round_->do_merge(*next_round_))) {
        STORAGE_LOG(WARN, "fail to do merge fragments of current round", K(ret));
      } else if (OB_FAIL(curr_round_->clean_up())) {
        STORAGE_LOG(WARN, "fail to do clean up of current round", K(ret));
      } else {
        std::swap(curr_round_, next_round_);
        const int64_t round_cost_time = common::ObTimeUtility::current_time() - start_time;
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

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::get_next_item(const T *&item)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  //} else if (is_empty_) {
  //  ret = common::OB_ITER_END;
  } else if (memory_sort_round_.has_data() && memory_sort_round_.is_in_memory()) {
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

template<typename T, typename Compare>
void MyExternalSort<T, Compare>::clean_up()
{
  int tmp_ret = common::OB_SUCCESS;
  is_inited_ = false;
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
    if (sort_rounds_[i].is_inited() && common::OB_SUCCESS != (tmp_ret = sort_rounds_[i].clean_up())) {
      STORAGE_LOG(WARN, "fail to clean up sort rounds", K(tmp_ret), K(i));
    }
  }
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::add_fragment_iter(ObFragmentIterator<T> *iter)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  } else if (OB_FAIL(curr_round_->add_fragment_iter(iter))) {
    STORAGE_LOG(WARN, "fail to add fragment iter");
  } else {
    is_empty_ = false;
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::get_current_round(ExternalSortRound *&curr_round)
{
  int ret = common::OB_SUCCESS;
  curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  } else if (NULL == curr_round_) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid current round", K(ret), KP(curr_round_));
  } else {
    curr_round = curr_round_;
  }
  return ret;
}

template<typename T, typename Compare>
int MyExternalSort<T, Compare>::transfer_final_sorted_fragment_iter(
    MyExternalSort<T, Compare> &merge_sorter)
{
  int ret = common::OB_SUCCESS;
  ExternalSortRound *curr_round = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "MyExternalSort has not been inited", K(ret));
  } else if (is_empty_) {
    ret = common::OB_SUCCESS;
  } else if (OB_FAIL(merge_sorter.get_current_round(curr_round))) {
    STORAGE_LOG(WARN, "fail to get current round", K(ret));
  } else if (NULL == curr_round) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner state", K(ret), KP(curr_round));
  } else if (memory_sort_round_.is_in_memory()) {
    if (OB_FAIL(memory_sort_round_.transfer_final_sorted_fragment_iter(*curr_round))) {
      STORAGE_LOG(WARN, "fail to transfer final sorted fragment iterator", K(ret));
    } else {
      merge_sorter.is_empty_ = false;
    }
  } else if (OB_FAIL(curr_round_->transfer_final_sorted_fragment_iter(*curr_round))) {
    STORAGE_LOG(WARN, "fail to get transfer sorted fragment iterator", K(ret));
  } else {
    merge_sorter.is_empty_ = false;
  }
  return ret;
}






}  // end namespace storage
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_PARALLEL_EXTERNAL_SORT_H_
