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
#define LEAF_PAGE_HEADER_SIZE 16  // why 16 instead of 12
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

  auto operator[](int index) const -> const MappingType &;

  /**
   * @brief 返回key对应的value， 好像还不如自己实现折半查找
   *
   * @param key
   * @param val 存储返回的value
   * @param comparator
   * @return false 没有查到
   */
  auto ValueOfKey(const KeyType &key, ValueType *val, const KeyComparator &comparator) const -> bool;

  /**
   * @brief 返回key对应的index
   *
   * @param key
   * @param index the index of key or the index of first key' that bigger than key
   * @return false表示不存在key，and the index is the index of first key' that bigger than key
   */
  auto IndexOfKey(const KeyType &key, const KeyComparator &comparator, int *index = nullptr) const -> bool;

  auto Back() const -> const MappingType & {
    assert(GetSize() >= 1);
    return array_[GetSize() - 1];
  }

  auto Front() const -> const MappingType & {
    assert(GetSize() >= 1);
    return array_[0];
  }

  void PopBack();
  void PopFront();
  void PushBack(const MappingType &val);
  void PushFront(const MappingType &val);

  /**
   * @brief 插入键值对到leaf node
   *
   * @param key
   * @param val
   * @param comparator
   * @return false 插入重复key会返回失败
   */
  auto Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator) -> bool;

  /**
   * @brief insert element into array_[index],
   *
   * @param index
   * @param key
   * @param val
   */
  void InsertAt(int index, const KeyType &key, const ValueType &val);

  void Split(BPlusTreeLeafPage *new_right_bro_node, page_id_t new_right_bro_page_id);

  void MergeToLeftBro(BPlusTreeLeafPage *left_bro_node);

  auto Delete(const KeyType &key, const KeyComparator &comparator) -> bool;

  void DeleteAt(int index);

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

 private:
  /**
   * @brief 移动下标[first, first + len)的键值对到dest的[start, start + len)，同时dest原来[start, end)的向后移
   * 只用于自己尾部元素移动到右兄弟顶端、自己首部元素移动到左兄弟尾端，保证移动后键值对有序。
   * like call len times of {dest->PushFront(Back()); PopBack();} or {dest->PushBack(Front()); PopFront();}
   * @param first
   * @param len
   * @param destination 目的节点
   * @param start 目的节点的开始下标
   */
  void MoveTo(int first, int len, BPlusTreeLeafPage *dest, int start);

 private:
  page_id_t next_page_id_;
  // FIXME(gukele) 类内初始值没用，压根没有构造函数，都是reinterpret_cast过来然后init()
  // page_id_t next_page_id_ = INVALID_PAGE_ID;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
