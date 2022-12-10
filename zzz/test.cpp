#include <algorithm>
#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <vector>

struct radix_value {
  int64_t id0;
  int32_t id1;
  radix_value() : id0(0), id1(0) {}
  bool operator<(const radix_value &other) const {
    if (id0 < other.id0)
      return true;
    else if (id0 == other.id0)
      return id1 < other.id1;
    else
      return false;
  }
};

struct t_struct {
  radix_value value;
  int radix_id;
};

template <typename T> struct RadixTraits {
  static const int nBytes = 12;
  static const int kRadixBits = 8;
  static const int kRadixThreshold = (1 << 10) * 4;
  static const int kRadixMask = (1 << kRadixBits) - 1;
  static const int kRadixBin = 1 << kRadixBits;

  using TP = T *;

  RadixTraits(int64_t min_id0, int32_t min_id1)
      : min_id0(min_id0), min_id1(min_id1){};

  int64_t min_id0;
  int32_t min_id1;

  inline int kth_byte(TP &x, int k) {
    int id;
    if (k >= 4) {
      uint64_t t_value = x->value.id0 - min_id0;
      id = (t_value >> ((k - 4) * kRadixBits)) & kRadixMask;
    } else {
      uint32_t t_value = x->value.id1 - min_id1;
      id = (t_value >> (k * kRadixBits)) & kRadixMask;
    }
    x->radix_id = id;
    return id;
  }
};

template <typename T>
void radix_sort(std::vector<T *> &list, int64_t min_id0, int32_t min_id1,
                int64_t max_id0, int32_t max_id1) {

  size_t size = list.size();
  uint64_t max_id0_ = max_id0 - min_id0;
  uint32_t max_id1_ = max_id1 - min_id1;
  int id0_round = 0;
  int id1_round = 0;
  while (max_id0_ > 0) {
    max_id0_ = max_id0_ >> RadixTraits<T>::kRadixBits;
    id0_round++;
  }
  while (max_id1_ > 0) {
    max_id1_ = max_id1_ >> RadixTraits<T>::kRadixBits;
    id1_round++;
  }

  RadixTraits<T> trait(min_id0, min_id1);
  std::vector<T *> t_list;
  t_list.assign(list.begin(), list.end());
  std::vector<T *> *prev_list = &list;
  std::vector<T *> *current_list = &t_list;
  for (size_t i = 0; i < RadixTraits<T>::nBytes; i++) {
    if (i < 4 && i >= id1_round)
      continue;
    if (i >= 4 && (i - 4) >= id0_round)
      continue;
    int bucket[RadixTraits<T>::kRadixBin] = {0};
    bool all_in_one = false;

    for (size_t j = 0; j < size; j++) {
      bucket[trait.kth_byte(current_list->at(j), i)]++;
    }

    for (size_t j = 0; j < RadixTraits<T>::kRadixBin; j++) {
      if (bucket[j] == size) {
        all_in_one = true;
        break;
      }
    }

    if (all_in_one) {
      continue;
    } else {
      for (size_t j = 1; j < RadixTraits<T>::kRadixBin; j++) {
        bucket[j] += bucket[j - 1];
        if (bucket[j] == size)
          break;
      }

      for (int j = size - 1; j >= 0; j--) {
        prev_list->at(--bucket[current_list->at(j)->radix_id]) =
            current_list->at(j);
      }
      std::swap(current_list, prev_list);
    }
  }
  list.assign(current_list->begin(),current_list->end());
}

int main() {
  const int num = 10000;
  int a1 = -100;
  int b1 = 100;
  int a2 = -100;
  int b2 = 100;
  srand((unsigned)time(NULL));
  t_struct t_structs[num];
  std::vector<t_struct *> list;
  int64_t min_id0 = INT64_MAX;
  int32_t min_id1 = INT32_MAX;
  int64_t max_id0 = INT64_MIN;
  int32_t max_id1 = INT32_MIN;
  for (int i = 0; i < num; i++) {
    t_structs[i].value.id0 = (rand() % (b1 - a1)) + a1;
    t_structs[i].value.id1 = (rand() % (b2 - a2)) + a2;
    min_id0 = std::min(min_id0, t_structs[i].value.id0);
    min_id1 = std::min(min_id1, t_structs[i].value.id1);
    max_id0 = std::max(max_id0, t_structs[i].value.id0);
    max_id1 = std::max(max_id1, t_structs[i].value.id1);
    list.push_back(&t_structs[i]);
  }
  radix_sort(list,min_id0,min_id1,max_id0,max_id1);
  for (int i = 0; i < list.size(); i++) {
    std::cout<<"id0="<<list[i]->value.id0<<", id1="<<list[i]->value.id1<<std::endl;
  }
  return 0;
}