#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

// 构造函数中就不应该
BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&other) noexcept
    : bpm_{other.bpm_}, page_{other.page_}, is_dirty_{other.is_dirty_} {
  other.ClearAllContents();
}

auto BasicPageGuard::operator=(BasicPageGuard &&other) noexcept -> BasicPageGuard & {
  // 释放this的资源
  Drop();
  // 移动other的资源
  bpm_ = other.bpm_;
  page_ = other.page_;
  is_dirty_ = other.is_dirty_;
  // other置空使其失效
  other.ClearAllContents();
  return *this;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    ClearAllContents();
  }
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

// dufault应该不太对
// ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept = default;
ReadPageGuard::ReadPageGuard(ReadPageGuard &&other) noexcept : guard_(std::move(other.guard_)) {
  // 清空内容后，other的析构调用Drop就不会释放锁了，我们这样避免other释放锁，然后this再加锁
  // 相当于直接吧other加的锁给了this
  // 初始化列表中BasicPageGuard的移动构造函数，让other已经失效了，其析构不会释放du
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // FIXED 移动赋值运算符，应该把自己的资源先drop掉
  Drop();
  guard_ = std::move(that.guard_);
  that.guard_.ClearAllContents();
  return *this;
};

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_{std::move(that.guard_)} {
  that.guard_.ClearAllContents();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  that.guard_.ClearAllContents();
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
