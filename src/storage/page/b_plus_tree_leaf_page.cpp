//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  if(next_page_id_ != INVALID_PAGE_ID) {
    return next_page_id_;
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  assert(next_page_id >= INVALID_PAGE_ID);
  if (next_page_id < INVALID_PAGE_ID) {
    throw Exception(ExceptionType::EXECUTION, "Cannot set other page id");
    next_page_id_ = INVALID_PAGE_ID;
    return;
  }
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType value{array_[index].second};
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueOfKey(const KeyType &key, ValueType *val, const KeyComparator& comparator) const -> bool {
  auto cmp = [&comparator](const MappingType &lhs, const KeyType &rhs) {
    return comparator(lhs.first, rhs) < 0;
  };

  auto it = std::lower_bound(array_, array_ + GetSize(), key, cmp);
  if(it == array_ + GetSize() || comparator(it->first, key) != 0) {
    return false;
  }

  *val = it->second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator& comparator) -> bool {
  int size = GetSize();

  if (size >= GetMaxSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Cannot insert to full leaf node");
    return false;
  }

  // 只支持unique key
  if(ValueOfKey(key, nullptr, comparator)) {
    return false;
  }

  // 插入算法
  int i = size - 1;
  for( ; i >= 0 ; --i) {
    if(comparator(KeyAt(i), key) < 0) {
      break;
    }
    // array_[i + 1] = std::move(array_[i]);
    array_[i + 1] = array_[i];
  }
  IncreaseSize(1);
  array_[i + 1] = {key, val};
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveTo(int start, BPlusTreeLeafPage *destination) {
  if(start >= GetSize() || start < 0) {
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

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
