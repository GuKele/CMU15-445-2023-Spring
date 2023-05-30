//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <iostream>
#include <iterator>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetMaxSize());
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(index >= 0 && index < GetMaxSize());
  array_[index].second = value;
}

/**
 * Helper method to get the index of value,用折半查找优化
 * TODO(gukele) 提前随便写一下
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
//   auto cmp = [](const MappingType& lhs, const ValueType& val) { return lhs.second < val; };
//   // KeyComparator cmp = [](const MappingType& lhs, const ValueType& val) { return lhs.second < val; };
//   auto it = std::lower_bound(&array_[0], &array_[GetSize()], value, cmp);
//   if(it->second == value) {
//     return it - array_;
//   }
//   return -1;
// }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Search(const KeyType &key, const KeyComparator& comparator, int *index) const -> ValueType {
  auto cmp = [&comparator](const MappingType &lhs, const MappingType &rhs) {
    return comparator(lhs.first, rhs.first) < 0;
  };
  auto it = std::upper_bound(array_ + 1, array_ + GetSize(), MappingType(key, ValueType()), cmp);

  // // 中间节点没有比key小的
  // if(it == array_ + GetSize()) {
  //   *index = GetSize() - 1;
  //   return Back().second;
  // }

  if(index != nullptr) {
    *index = it - 1 - array_;
  }
  return (it - 1)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator& comparator) {
  int i = GetSize() - 1;
  for( ; i > 0 ; --i) {
    if(comparator(KeyAt(i), key) < 0) {
      break;
    }
    // array_[i + 1] = std::move(array_[i]);
    array_[i + 1] = array_[i];

    // if(comparator(KeyAt(i), key) > 0) {
    //   array_[i + 1] = array_[i];
    // } else {
    //   IncreaseSize(1);
    //   array_[i + 1] = {key, value};
    //   return;
    // }
  }

  // // 没有比key小的key,所以应该插入到1的位置
  // if(comparator(KeyAt(1), key) > 0) {
  //   IncreaseSize(1);
  //   array_[1] = {key, value};
  // }

  IncreaseSize(1);
  array_[i + 1] = {key, value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveTo(int start, BPlusTreeInternalPage *destination) {
  if(start >= GetSize() || start < 1) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "error start");
  }
  if(!destination->IsEmpty()) {
    throw Exception(ExceptionType::EXECUTION, "Cannot move to non-empty leaf node");
  }

  int n = GetSize();
  for(int i = 0 ; i + start < n ; ++i) {
    destination->array_[i] = array_[start + i];
  }
  SetSize(start);
  destination->SetSize(n - start);
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
//   for(int i = GetSize() - 1 ; i >= 0 ; --i) {
//     if(comparator(KeyAt(i), key) < 0) {
//       array_[i + 1] = {key, value};
//       return;
//     }
//     array_[i + 1] = array_[i];
//   }
// }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
