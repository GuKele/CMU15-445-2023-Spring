/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BasicPageGuard &&guard, BufferPoolManager *bpm, int index)
    : index_(index), bpm_{bpm}, guard_{std::move(guard)} {
  if (guard_.IsValid()) {
    leaf_node_ = guard_.As<LeafPage>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &that)
    : index_{that.index_}, leaf_node_{that.leaf_node_}, bpm_{that.bpm_} {
  if (that.guard_.IsValid()) {
    guard_ = bpm_->FetchPageBasic(that.guard_.PageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(const IndexIterator &that) -> IndexIterator & {
  index_ = that.index_;
  leaf_node_ = that.leaf_node_;
  bpm_ = that.bpm_;
  if (that.guard_.IsValid()) {
    guard_ = bpm_->FetchPageBasic(that.guard_.PageId());
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator &&that) noexcept
    : index_{that.index_}, leaf_node_{that.leaf_node_}, bpm_{that.bpm_}, guard_{std::move(that.guard_)} {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(IndexIterator &&that) noexcept -> IndexIterator & {
  index_ = that.index_;
  leaf_node_ = that.leaf_node_;
  bpm_ = that.bpm_;
  guard_ = std::move(that.guard_);
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // throw std::runtime_error("unimplemented");
  // TODO(gukele) 根据operator++来改
  return leaf_node_ == nullptr && index_ == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // throw std::runtime_error("unimplemented");
  return (*leaf_node_)[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator->() -> const MappingType * { return &this->operator*(); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // throw std::runtime_error("unimplemented");

  // 已经是end了
  if (index_ == -1 && leaf_node_ == nullptr) {
    // BUSTUB_ASSERT(leaf_node_ == nullptr, "End iterator should nullptr && index_ = -1");
    index_ = -1;
    leaf_node_ = nullptr;
    return *this;
  }

  if (index_ < leaf_node_->GetSize() - 1) {
    ++index_;
  } else if (auto next_page_id = leaf_node_->GetNextPageId(); next_page_id != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageBasic(next_page_id);
    leaf_node_ = guard_.As<LeafPage>();
    index_ = 0;
  } else {
    index_ = -1;
    leaf_node_ = nullptr;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
