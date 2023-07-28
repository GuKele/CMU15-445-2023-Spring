//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 *
 *               3 (k2)       7 (k3)        9 (k4)
 *  pid1(-∞,3)   pid2[3,7)    pid3[7,9)     pid4[9,+∞)
 *
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 *
 *  Header format (size in byte, 12 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Deleted to disallow initialization
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid BPlusTreeInternalPage
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = INTERNAL_PAGE_SIZE);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);

  /**
   *
   * @param value the value to search for
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> ValueType;

  auto FrontValue() const -> const ValueType & {
    assert(GetSize() >= 1);
    return array_[0].second;
  }

  auto Back() const -> const MappingType & {
    assert(GetSize() >= 1);
    return array_[GetSize() - 1];
  }

  void PopBack();
  void PopFront();
  void PushBack(const MappingType &val);
  // not real std::forward(argv) and call ctor,just notice arguments
  void EmplaceBack(const KeyType &key, const ValueType &val);
  // void PushFront(const MappingType &val);
  // void EmplaceFront(const KeyType &key, const ValueType &val);
  void PushFrontValue(const ValueType &val);
  // void EmplaceFront(const ValueType &val);

  /**
   * @brief 返回key可能所在对应的page_id，本项目中的B+树非叶子节点是k(i) <= k <
   * k(i+1) 对于如下internal node，第一行是key,第二行是value(page id)
   *
   *               3 (k2)       7 (k3)        9 (k4)
   *  pid1(-∞,3)   pid2[3,7)    pid3[7,9)     pid4[9,+∞)
   *
   * 则查找key==5的时候，返回pid2,index是key==3对应的indxe,即index==1
   * @param key
   * @param index 返回key对应的下标(刚好比key小或者等于key的下标)
   * @return ValueType
   */
  auto Search(const KeyType &key, const KeyComparator &comparator, int *index = nullptr) const -> ValueType;

  // /**
  //  * @brief 插入key-value(page_id)，保证插入前不满。
  //  *
  //  * @param key
  //  * @param value
  //  * @return int
  //  */
  // auto Insert(const KeyType &key, const ValueType &value, const KeyComparator& comparator) -> int;

  /**
   * @brief 插入key-value(page_id)，插入前需要保证不满，并且保证不插入重复key,并且只是单纯的internal node中插入
   *
   * @param key
   * @return auto
   */
  void Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);

  void InsertAt(int index, const KeyType &key, const ValueType &value);

  // /**
  //  * @brief 移动下标[start, end)的键值对到空的destination，注意并没有将destination的key1置无效
  //  *
  //  * @param start 移动开始的下标
  //  * @param destination 空BPlusTreeLeafPage目的地
  //  */
  // void MoveTo(int start, BPlusTreeInternalPage *destination);

  void MergeToLeftBro(BPlusTreeInternalPage *left_bro_node);

  /**
   * @brief 移动下标[first, first + len)的键值对到dest的[start, start + len)，同时dest原来[start, end)的向后移
   * 只用于自己尾部元素移动到右兄弟顶端、自己首部元素移动到左兄弟尾端，保证移动后键值对有序。
   * like call len times of {dest->PushFront(Back()); PopBack();} or {dest->PushBack(Front()); PopFront();}
   * @param first
   * @param len
   * @param destination 目的节点
   * @param start 目的节点的开始下标
   */
  void MoveTo(int first, int len, BPlusTreeInternalPage *dest, int start);

  void DeleteAt(int index);

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
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
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
