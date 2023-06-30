//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  // default ctor create end iterator
  IndexIterator();
  ~IndexIterator();  // NOLINT

  IndexIterator(ReadPageGuard &&guard, BufferPoolManager *bpm, int index);

  IndexIterator(const IndexIterator &that);
  auto operator=(const IndexIterator &that) -> IndexIterator &;

  IndexIterator(IndexIterator &&that) noexcept;
  auto operator=(IndexIterator &&that) noexcept -> IndexIterator &;

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator->() -> const MappingType *;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &iter) const -> bool {
    // throw std::runtime_error("unimplemented");
    return (index_ == iter.index_ && leaf_node_ == iter.leaf_node_);
  }

  auto operator<(const IndexIterator &iter) const -> bool {
    throw Exception(ExceptionType::EXECUTION, "IndexIterator operator<() not implemented");
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    // throw std::runtime_error("unimplemented");
    return !(*this == itr);
  }

 private:
  // add your own private member variables here
  int index_{-1};
  const LeafPage *leaf_node_{nullptr};
  BufferPoolManager *bpm_;
  // TODO(gukele): should the iterator hold PageGuard？
  ReadPageGuard guard_;
};

}  // namespace bustub
