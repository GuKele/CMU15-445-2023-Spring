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
#include "common/macros.h"
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
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  if (next_page_id_ != INVALID_PAGE_ID) {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::operator[](int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueOfKey(const KeyType &key, ValueType *val, const KeyComparator &comparator) const
    -> bool {
  if (int index; IndexOfKey(key, comparator, &index)) {
    *val = ValueAt(index);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOfKey(const KeyType &key, const KeyComparator &comparator, int *index) const
    -> bool {
  auto cmp = [&comparator](const MappingType &lhs, const KeyType &rhs) { return comparator(lhs.first, rhs) < 0; };

  auto it = std::lower_bound(array_, array_ + GetSize(), key, cmp);

  if (index != nullptr) {
    *index = it - array_;
  }

  return static_cast<bool>(it != array_ + GetSize() && comparator(it->first, key) == 0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() {
  if (!IsEmpty()) {
    IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() {
  int n = GetSize();
  if (!IsEmpty()) {
    IncreaseSize(-1);
  }
  for (int i = 1; i < n; ++i) {
    array_[i - 1] = array_[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(const MappingType &val) {
  assert(!IsFull());
  array_[GetSize()] = val;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(const MappingType &val) {
  assert(!IsFull());
  int n = GetSize();
  for (int i = n; i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = val;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator)
    -> bool {
  int n = GetSize();

  if (n >= GetMaxSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Cannot insert to full leaf node");
    return false;
  }

  // 只支持unique key
  int index;
  if (IndexOfKey(key, comparator, &index)) {
    return false;
  }

  // 插入算法
  // int i = size - 1;
  // for( ; i >= 0 ; --i) {
  //   if(comparator(KeyAt(i), key) < 0) {
  //     break;
  //   }
  //   // array_[i + 1] = std::move(array_[i]);
  //   array_[i + 1] = array_[i];
  // }
  // IncreaseSize(1);
  // array_[i + 1] = {key, val};

  InsertAt(index, key, val);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &val) {
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  IncreaseSize(1);
  array_[index] = {key, val};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *new_right_bro_node, page_id_t new_right_bro_page_id) {
  auto mid = GetMinSize();
  MoveTo(mid, GetSize() - mid, new_right_bro_node, 0);
  new_right_bro_node->SetNextPageId(GetNextPageId());
  SetNextPageId(new_right_bro_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeToLeftBro(BPlusTreeLeafPage *left_bro_node) {
  MoveTo(0, GetSize(), left_bro_node, left_bro_node->GetSize());
  left_bro_node->SetNextPageId(GetNextPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveTo(int first, int len, BPlusTreeLeafPage *dest, int start) {
  // 也可能会出现len = 0的情况，所以没必要在这里判断
  // if (first >= GetSize() || first < 0 || len <= 0 || len > GetSize() || start < 0 || start > dest->GetSize()) {
  //   throw Exception(ExceptionType::OUT_OF_RANGE, "error start");
  // }
  assert((start == 0 && first + len == GetSize()) || start == dest->GetSize());

  // dest腾出[start, start + len)的位置
  for (int i = dest->GetSize() - 1; i >= start; --i) {
    // TODO(gukele) std::move？不知道key中是否存在移动高效的类型
    dest->array_[i + len] = dest->array_[i];
  }
  // [first, first + len）元素移动到dest的[start, start + len)
  for (int i = 0; i < len; ++i) {
    dest->array_[start + i] = array_[first + i];
  }
  dest->IncreaseSize(len);

  // [first + len, GetSize())的元素向前移动len位置
  for (int i = 0; i < GetSize() - first - len; ++i) {
    array_[first + i] = array_[first + len + i];
  }
  IncreaseSize(-len);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (int index; IndexOfKey(key, comparator, &index)) {
    DeleteAt(index);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteAt(int index) {
  for (int i = index + 1; i < GetSize(); ++i) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
