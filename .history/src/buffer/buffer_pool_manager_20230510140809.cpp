//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];

  // 可以看到其实frame和pages_数量是一样的，是一个一一对应的关系，frame其实就是pages_的下标
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  return NewPageImpl(page_id);
}

auto BufferPoolManager::NewPageImpl(page_id_t *page_id) -> Page * {
  if (frame_id_t frame_id; GetAvailFrame(&frame_id)) {
    *page_id = AllocatePage();

    AddPage(*page_id, frame_id);

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::GetAvailFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    // free_list_'s frame is already bzero
    return true;
  }

  if (replacer_->Evict(frame_id)) {
    // 淘汰的框架其实是执行一个删除的page的操作,
    // TODO(gukele): 已经evict了，但是在delete中又再次remove,有些重复浪费了，但是为了代码可读性
    DeletePageImpe(*frame_id);

    return true;
  }

  return false;
}

inline void BufferPoolManager::ResetPageMetadata(Page *page) {
  page->pin_count_ = 0;
  page->page_id_ = 0;
  page->is_dirty_ = false;
  // page->rwlatch_;
}

void BufferPoolManager::AddPage(page_id_t page_id, frame_id_t frame_id) {
  auto &page = pages_[frame_id];
  page.page_id_ = page_id;
  page_table_.insert({page_id, frame_id});

  replacer_->RecordAccess(frame_id);
  PinPageImpl(frame_id);
}

void BufferPoolManager::DeletePageImpe(frame_id_t frame_id) {
  auto &page = pages_[frame_id];
  // 刷盘
  if (page.IsDirty()) {
    FlushPageImpl(page.GetPageId());
  }
  // page_table——中删除
  page_table_.erase(page.GetPageId());
  // 重置memory
  page.ResetMemory();
  // 重置 metadata
  ResetPageMetadata(&page);
  // 从 replacer中删除
  replacer_->Remove(frame_id);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    PinPageImpl(it->second);
    return &pages_[it->second];
  }

  // 得到一个新的可用的frame,从磁盘读入page到该frame
  if (frame_id_t frame_id; GetAvailFrame(&frame_id)) {
    AddPage(page_id, frame_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end() && pages_[it->second].GetPinCount() > 0) {
    auto frame_id = it->second;
    --pages_[frame_id].pin_count_;
    // TODO(gukele): 放在这里？is_dirty是用或来设置的，因为多次fecth页面，只要有一次f e t ch
    pages_[frame_id].is_dirty_ |= is_dirty;
    if (pages_[frame_id].GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, true);
      // pages_[frame_id].is_dirty_ |= is_dirty;
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::PinPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    auto frame_id = it->second;
    PinPageImpl(frame_id);
    return true;
  }
  return false;
}

inline void BufferPoolManager::PinPageImpl(frame_id_t frame_id) {
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, false);
  }
  ++pages_[frame_id].pin_count_;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  return FlushPageImpl(page_id);
}

auto BufferPoolManager::FlushPageImpl(page_id_t page_id) -> bool {
  // c++17语法糖,if中定义变量
  // if(auto obj = xx ; bool)
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    auto frame_id = it->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guard(latch_);
  for (auto [page_id, frame_id] : page_table_) {
    // TODO(gukele): maybe this flush all  dont care is or not dirty
    if (pages_[frame_id].IsDirty()) {
      // FlushPageImpl(page_id);
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  auto frame_id = it->second;
  if (auto &page = pages_[frame_id]; page.GetPinCount() == 0) {
    // TODO(gukele):  没有搞懂这个函数，只是照着描述写了一下
    DeletePageImpe(frame_id);

    free_list_.push_back(frame_id);

    DeallocatePage(page_id);
    return true;
  }

  return false;
}

inline auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  // 明明可能会失败啊
  if (page != nullptr) {
    return {this, page};
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
