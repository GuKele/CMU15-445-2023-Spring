//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int max_size) { max_size_ = max_size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
// TODO(gukele) generally!!!!!
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsLeafPage()) {
    return max_size_ / 2;
  }
  if (page_type_ == IndexPageType::INTERNAL_PAGE) {
    return (max_size_ + 1) / 2;
  }
  return -1;

  // return max_size_ / 2;
}

auto BPlusTreePage::IsFull() const -> bool { return GetSize() == GetMaxSize(); }

auto BPlusTreePage::IsEmpty() const -> bool {
  if (IsLeafPage()) {
    return GetSize() == 0;
  }
  // internal node没key的时候，只有第一个val,就为空
  return GetSize() == 1;
}

auto BPlusTreePage::IsSafe() const -> bool { return GetSize() >= GetMinSize(); }

auto BPlusTreePage::IsSafeAfterOption(BPlusTreeOption opt) const -> bool {
  // INSERT
  if (opt == BPlusTreeOption::INSERT) {
    if (IsLeafPage()) {  // 叶子节点是插入后满则分裂，所以插入后不达到GetMaxSize就是安全
      return GetSize() + 1 < GetMaxSize();
    }
    return !IsFull();  // 非叶子节点是插入前满则分裂
  }

  // DELETE
  return GetSize() - 1 >= GetMinSize();
}

}  // namespace bustub
