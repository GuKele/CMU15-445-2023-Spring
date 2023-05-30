//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief 返回key对应的value， 好像还不如自己实现折半查找
   *
   * @param key
   * @param val 存储返回的value
   * @param comparator
   * @return false 没有查到
   */
  auto ValueOfKey(const KeyType &key, ValueType *val, const KeyComparator& comparator) const -> bool;

  /**
   * @brief 插入键值对到leaf node
   *
   * @param key
   * @param val
   * @param comparator
   * @return false 插入重复key会返回失败
   */
  auto Insert(const KeyType &key, const ValueType &val, const KeyComparator& comparator) -> bool;

  /**
   * @brief 移动下标[start, end)的键值对到空的destination
   *
   * @param start 移动开始的下标
   * @param destination 空BPlusTreeLeafPage目的地
   */
  void MoveTo(int start, BPlusTreeLeafPage *destination);


  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

  auto Back() const -> MappingType {
    assert(GetSize() >= 1);
    return array_[GetSize() - 1];
  }

 private:
  page_id_t next_page_id_ = INVALID_PAGE_ID;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
